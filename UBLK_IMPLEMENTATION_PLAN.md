# UBLK Implementation Plan

## Track 1: libublk kernel driver integration (blockyard-ublk crate)

### Goal
Wire up the actual Linux ublk kernel interface so `UblkDevice::start()` creates a real `/dev/ublkbN` block device, and IO requests from the kernel are dispatched to the `BlockHandler` trait.

### What exists
- `crates/blockyard-ublk/src/ublk.rs` — has `UblkDevice<H: BlockHandler>`, `IoRequest`, `IoOperation`, `BlockHandler` trait
- `UblkDevice::start()` is a stub (just sets a boolean)
- `UblkDevice::submit_io()` dispatches to handler (works for tests)
- The crate already has `WritePipeline`, `EcWritePipeline`, `MetadataCache`, `LeaseManager`, `ClientSession`, etc.

### What to implement
1. Add `libublk` dependency to `crates/blockyard-ublk/Cargo.toml`
2. In `ublk.rs`, implement `start()` to:
   - Call `libublk::ctrl::UblkCtrl::new()` to create a ublk control device
   - Configure queue depth, block size, device size from `UblkDeviceConfig`
   - Register the device with the kernel
   - Spawn IO handling loop that reads ublk IO commands and dispatches to `BlockHandler::handle_io()`
   - Store the ublk device path (e.g. `/dev/ublkb0`) for the caller
3. Implement `stop()` to tear down the ublk device
4. Add `device_path()` method that returns the `/dev/ublkbN` path
5. Keep backward compat — the existing mock path should still work for unit tests

### Key constraint
- libublk requires Linux kernel 6.0+ with `ublk_drv` module loaded
- Tests that exercise real ublk need root (or io_uring capabilities)
- Unit tests should continue to use the mock path without libublk

## Track 2: BlockHandler implementation that wires ublk to pipelines

### Goal
Create a real `BlockHandler` implementation that translates kernel IO requests into WritePipeline/ReadPipeline/EcWritePipeline/EcReadPipeline calls against a live cluster.

### What exists
- `BlockHandler` trait in `ublk.rs` with `handle_io(&self, IoRequest) -> Result<Option<Bytes>, Error>`
- `WritePipeline` in `write_pipeline.rs` — full replicated write path
- `EcWritePipeline` in `ec_write_pipeline.rs` — erasure-coded write path  
- `ReadPipeline` in `crates/blockyard-client/src/pipeline.rs` — replicated read path
- `EcReadPipeline` in `crates/blockyard-client/src/ec_read_pipeline.rs` — EC read path
- `TcpDataNodeClient` in `tcp_client.rs` — real TCP data node client
- `HttpMetadataClient` in `http_metadata_client.rs` — real HTTP metadata client
- `LeaseManager` in `lease_manager.rs` — volume write lease management
- `ClientSession` in `session.rs` — session + operation ID generation

### What to implement
Create `crates/blockyard-ublk/src/block_handler.rs`:
1. `ClusterBlockHandler` struct that holds:
   - `WritePipeline<TcpDataNodeClient, HttpMetadataClient>` for replicated writes
   - `ReadPipeline<...>` for replicated reads
   - `EcWritePipeline<...>` for EC writes (when volume uses EC protection)
   - `EcReadPipeline<...>` for EC reads
   - `LeaseManager` for write lease
   - `ClientSession` for operation IDs
   - `MetadataCache` for extent mappings
   - Volume metadata (protection policy, size, block size)
2. Implement `BlockHandler` for `ClusterBlockHandler`:
   - `IoOperation::Write` → WritePipeline::write() or EcWritePipeline::write() based on protection
   - `IoOperation::Read` → ReadPipeline::read() or EcReadPipeline::read() based on protection
   - `IoOperation::Flush` → ensure all pending writes are committed
   - `IoOperation::Discard` → no-op or mark extents as discarded

## Track 3: CLI mount/unmount commands

### Goal
Make `byard mount <VOLUME_ID>` and `byard unmount <VOLUME_ID>` actually work end-to-end.

### What exists
- `crates/blockyard-cli/src/commands.rs` has mount/unmount subcommands defined
- `crates/blockyard-cli/src/cli.rs` has the command dispatch
- The CLI already connects to the management API via HTTP

### What to implement
1. `mount` command flow:
   - Fetch volume metadata from mgmt API (size, protection policy)
   - Acquire write lease via mgmt API
   - Create `ClusterBlockHandler` with real TCP/HTTP clients
   - Create `UblkDevice` with the handler
   - Start the ublk device → get `/dev/ublkbN` path
   - Print the device path
   - Keep running (foreground) until SIGTERM/SIGINT
   - On shutdown: stop ublk device, release lease
2. `unmount` command:
   - Signal the mount process to stop
   - Or directly remove the ublk device via libublk control

## Track 4: Integration tests with real filesystem + fault injection

### Goal
End-to-end tests that create a ublk device, format with a filesystem, write/read data, and verify correctness under fault injection.

### Test categories

#### 4a. Basic mount + filesystem tests
- Mount a replicated volume (replicas=1), format ext4, write file, read back, verify
- Mount a replicated volume (replicas=2), same test
- Mount a replicated volume (replicas=3), same test  
- Mount an EC volume (k=2, m=1), format ext4, write file, read back, verify

#### 4b. Jepsen-style fault injection
- **Node failure during write**: Kill a data node while writing, verify data integrity after recovery
- **Leader failure**: Kill the raft leader, wait for new election, verify volume still accessible
- **Network partition**: Use iptables to partition a node, write data, heal partition, verify consistency
- **Disk failure**: Corrupt extent data on one replica's disk, verify scrub detects + repairs
- **Concurrent writers**: Two clients mount the same volume (should be prevented by lease)
- **Crash recovery**: Kill the mount client mid-write, remount, verify fs consistency (fsck)

#### 4c. Erasure coding specific
- EC volume (k=2, m=1): lose 1 data node, verify reads still succeed via reconstruction
- EC volume (k=4, m=2): lose 2 nodes, verify reads still succeed
- EC volume: corrupt one fragment, verify read pipeline detects + reconstructs
- EC write coalescing: many small writes, verify stripe assembly + fragment distribution

#### 4d. Replication consistency  
- Write data with replicas=3, verify all replicas have identical data
- Kill one replica, write more data, bring replica back, verify catch-up
- Epoch change during write (stale epoch handling)
- Write watermark enforcement (read-your-own-writes)

### Test infrastructure needed
- `TestCluster` harness that starts N blockyard nodes in separate tempdirs
- Ability to start/stop/kill individual nodes
- Ability to inject network faults (iptables or TCP proxy)
- Ability to corrupt data on specific nodes' disks
- Root or CAP_SYS_ADMIN for ublk device creation
- Tests must be `#[ignore]` by default (need root + ublk kernel module)
