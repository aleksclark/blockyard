# UBLK Mount E2E Implementation Plan

## Overview

Make `byard mount <VOLUME_ID>` work end-to-end: create a real `/dev/ublkbN` block device
backed by the blockyard cluster, so users can `mkfs.ext4 /dev/ublkbN && mount /dev/ublkbN /mnt`.

Also fix integration tests that claim to be "real" but use mocks.

## Track 1: TCP Read Path + ClusterBlockHandler Read Wiring

### Problem
- `TcpDataNodeClient` only has `write_extent()` — no `read_extent()`
- `ClusterBlockHandler::handle_read()` finds the extent mapping but returns zeros
- Server-side `DataPlaneServer` already handles ReadReq and sends payload back
- `ReadPipeline` in blockyard-client already works with mock readers

### Implementation

**1a. Add `read_extent` to `TcpDataNodeClient`** (`crates/blockyard-ublk/src/tcp_client.rs`)

The wire protocol already supports reads — `DataPlaneServer` handles `ProtocolMessage::ReadReq`.

Add to `TcpDataNodeClient`:
```rust
pub async fn read_extent(
    &self,
    node_id: NodeId,
    volume_id: VolumeId,
    extent_id: ExtentId,
    extent_version: u64,
    offset: u64,
    length: u64,
) -> Result<Bytes, DataNodeError> {
    // 1. Get/create pooled connection for node_id
    // 2. Build ReadExtentRequest { volume_id, extent_id, extent_version, offset, length }
    // 3. Send ProtocolMessage::ReadReq frame
    // 4. Read response frame -> ProtocolMessage::ReadResp
    // 5. If resp.success, read resp.payload_size bytes of raw data
    // 6. Return Bytes
}
```

Also implement the `DataNodeReader` trait from blockyard-client on `TcpDataNodeClient`:
```rust
impl DataNodeReader for TcpDataNodeClient {
    async fn read_extent(&self, node_id: NodeId, ...) -> Result<ReadResult, ReadError> { ... }
}
```

**1b. Wire ReadPipeline into ClusterBlockHandler** (`crates/blockyard-ublk/src/block_handler.rs`)

Currently `ClusterBlockHandler<D, M>` is generic over `D: DataNodeClient` and `M: MetadataClient`.
Change it to also take read-path generics:

```rust
pub struct ClusterBlockHandler<D, M, R, S> {
    write_pipeline: WritePipeline<D, M>,
    ec_write_pipeline: Option<EcWritePipeline<D, M>>,
    read_pipeline: ReadPipeline<M, R, HealthReporterImpl, S>,
    ec_read_pipeline: Option<EcReadPipeline<M, R, HealthReporterImpl, S>>,
    // ... existing fields
}
```

In `handle_read()`:
```rust
// Instead of returning zeros:
let request = ReadRequest { volume_id, extent_id, offset, length };
match &self.config.protection {
    Replicated { .. } => self.read_pipeline.read(request).await,
    ErasureCoded { .. } => self.ec_read_pipeline.read(request, ec_mapping).await,
}
```

ALTERNATIVE (simpler): Since `TcpDataNodeClient` will implement both `DataNodeClient` (write) and `DataNodeReader` (read), and `HttpMetadataClient` implements both `MetadataClient` and `MetadataProvider`, we can keep `ClusterBlockHandler<D, M>` but add bounds `D: DataNodeClient + DataNodeReader` and `M: MetadataClient + MetadataProvider`. Then construct the ReadPipeline inside the handler.

### Tests
- Add integration test in `crates/blockyard-test-harness/tests/full_stack.rs` that does write_extent + read_extent through TCP
- Update ClusterBlockHandler tests to verify read path returns real data (not zeros)

---

## Track 2: CLI Mount/Unmount Commands

### Problem
- `HttpClient::mount()` returns "not yet supported via HTTP"
- Mount should be a CLIENT-SIDE operation, not server-side
- The mount command needs to: connect to cluster, fetch volume info, acquire lease, build all pipeline components, create UblkDevice with ClusterBlockHandler, start kernel mode, handle signals

### Implementation

**2a. Refactor mount to be client-side** (`crates/blockyard-cli/src/commands.rs`)

Mount doesn't need a server-side endpoint. The client:
1. Fetches volume info via mgmt API (`GET /api/v1/volumes/{id}`)
2. Fetches cluster info (nodes, epoch) via mgmt API
3. Acquires write lease via mgmt API (`POST /api/v1/leases/acquire`)
4. Constructs `TcpDataNodeClient` with node addresses
5. Constructs `HttpMetadataClient` pointing at mgmt API
6. Constructs `MetadataCache` and populates it
7. Constructs `LeaseManager` for background renewal
8. Constructs `WritePipeline` (or `EcWritePipeline` depending on protection)
9. Constructs `ReadPipeline` (or `EcReadPipeline`)
10. Constructs `ClusterBlockHandler` with all of the above
11. Creates `UblkDevice` with the handler + volume config (size, block size)
12. Calls `start_kernel()` to create `/dev/ublkbN`
13. Prints device path
14. Blocks on SIGTERM/SIGINT
15. On signal: calls `stop()`, releases lease, exits

**2b. Update CLI client trait** (`crates/blockyard-cli/src/client.rs`)

Remove `mount()` and `unmount()` from `BlockyardClient` trait — mount is not an API call,
it's a local operation. The mount command directly constructs the UBLK device.

OR: keep the trait method but implement it in a `LocalMountClient` that wraps the real
UBLK logic. The `HttpClient` version stays as "not supported" and we add a new impl.

Simpler: just bypass the `BlockyardClient` trait for mount. In `execute_mount()`, accept
the endpoint URL directly and do the whole thing inline.

**2c. Unmount** (`crates/blockyard-cli/src/commands.rs`)

Two options:
- `byard unmount <VOLUME_ID>` sends a signal to the mount process (needs PID tracking)
- `byard unmount <DEVICE>` directly removes the ublk device via libublk control

Simpler for now: the mount process handles SIGTERM gracefully (releases lease, stops device).
Unmount = `kill $(pgrep -f "byard mount VOLUME_ID")` or `byard unmount` uses libublk to
list and remove the device.

### Dependencies
- Track 1 must be done first (need working read path for the handler)

---

## Track 3: Fix Integration Tests

### Problem Files

**tests/ublk_client.rs** (3 tests) — Claims UBLK client testing, uses:
- `DiskBackedTestDataNode` — in-memory HashMap of TempDir-backed stores per NodeId
- `TestMetadataClient` — pure in-memory mock
- `setup_test_pipeline` helper — wires up all fakes
- "Crash" is just dropping a pipeline struct
FIX: Use `RealProcessCluster` from the test harness. Spawn real processes, use real TCP.

**tests/consistency.rs** (6 tests, 5 use mocks) — Claims consistency testing:
- 1 test uses real in-memory Raft cluster (keep this one)
- 5 tests use MockMetadataProvider, MockDataNodeReader, DiskBackedTestDataNode
FIX: Move mock-based tests to unit tests in their respective crates. For integration:
use RealProcessCluster to test majority-ack, read-your-own-writes, stale epoch with real nodes.

**tests/observability.rs** (6 tests, 5 use mocks):
- MockDataNodeClient, TestMetadataClient, FakeExtentReader
FIX: These are really unit tests for metric recording. Move to crate-level unit tests.
Add 1-2 integration tests that verify metrics are emitted from a real cluster operation.

**tests/auth.rs** (4 tests):
- Pure in-memory data structure tests
FIX: Move to unit tests in blockyard-common. Add integration test that verifies auth
rejection when a real client sends bad credentials.

### Approach
- Don't delete any test logic — move mock-based tests to unit test modules
- Replace integration-level tests with RealProcessCluster-based versions
- Use the existing pattern from `tests/replication_e2e.rs` and `tests/fault_injection.rs`

---

## Execution Order

1. Track 1a: TcpDataNodeClient read_extent (no dependencies)
2. Track 1b: ClusterBlockHandler read wiring (depends on 1a)
3. Track 2: CLI mount/unmount (depends on 1b)
4. Track 3: Fix tests (independent, can run in parallel with 1-2)

## Build & Verify

```bash
cargo test --workspace          # All unit tests pass
cargo test --workspace --features ublk-kernel   # UBLK kernel tests (needs root + ublk module)
cargo build --release -p blockyard-cli          # CLI builds
```
