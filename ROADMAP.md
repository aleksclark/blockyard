# Blockyard Implementation Roadmap

Implementation plan derived from `blockyard_client_data_node_spec.md`. Phases are ordered by dependency: foundational crates first, then data path, then cluster coordination, then hardening.

Legend: `[ ]` not started · `[-]` in progress · `[x]` done

---

## Phase 0 — Project Skeleton & Shared Types

Bootstrap the workspace, establish crate boundaries, and define the type vocabulary used by every subsequent phase.

- [x] **P0.1** Create Cargo workspace with crates: `blockyard` (bin), `blockyard-cli` (bin), `blockyard-common`, `blockyard-gossip`, `blockyard-protocol`, `blockyard-raft`, `blockyard-storage`, `blockyard-ublk`
- [x] **P0.2** Define core ID types in `blockyard-common`: `NodeId`, `VolumeId`, `ExtentId`, `DiskId`, `SessionId`, `OperationId`, `EpochId`, `RaftGroupId`
- [x] **P0.3** Define `ProtectionPolicy` enum (replication factor N, erasure coding K+M) — §2.10
- [x] **P0.4** Define `DiskState` enum (`healthy`, `suspect`, `degraded`, `draining`, `failed`, `removed`) — §2.9, §5.8.1
- [x] **P0.5** Define shared error types with `thiserror`; binary crates use `anyhow`
- [x] **P0.6** Define config structs (`NodeConfig`, `StorageSection`, `RaftSection`, `GossipSection`, `ProtocolSection`, `TlsSection`, `AuthSection`) with TOML deserialization
- [x] **P0.7** Set up `tracing` subscriber initialization for both binaries
- [x] **P0.8** CI pipeline: `cargo fmt`, `cargo clippy`, `cargo test`, coverage gate ≥95%

---

## Phase 1 — Local Disk Model & Extent Storage

Build the data node's local storage layer: per-disk XFS management, extent file lifecycle, integrity metadata, and crash-consistent durability.

### 1A — Disk Inventory & Health

- [x] **P1A.1** Disk discovery: detect physical disks, assign/recover persistent `DiskId` — §5.2, §5.10
- [x] **P1A.2** XFS filesystem validation per disk (exactly one dedicated XFS per disk) — §3.3, §5.10.3
- [x] **P1A.3** `DiskState` machine with transition rules and policy-driven derivation from telemetry — §5.8, §5.8.1
- [x] **P1A.4** Per-disk telemetry collection: read/write errors, checksum mismatches, media errors, timeouts, temperature, latency outliers — §5.8
- [x] **P1A.5** Allocation guards: refuse new extents on `degraded`/`draining`/`failed`/`removed` disks — §5.2, invariant 6
- [x] **P1A.6** Bad-region map: track localized failures, quarantine regions, report affected extents — §5.9
- [x] **P1A.7** Disk qualification state for newly added disks (burn-in before `healthy`) — §5.10.5

### 1B — Extent File Lifecycle

- [x] **P1B.1** Extent file layout on XFS: path scheme from `(DiskId, ExtentId, ExtentVersion)` — §5.2
- [x] **P1B.2** Write staging: temporary file creation, payload write, integrity metadata (strong checksum) — §5.3, §5.4
- [x] **P1B.3** Local durability: `fsync`/`fdatasync`/`O_DSYNC` with crash-consistent guarantees — §5.4, invariant 9
- [x] **P1B.4** Atomic promotion from staged → committed local extent (rename after fsync) — §5.3
- [x] **P1B.5** Immutability enforcement: committed extent files are never overwritten — §5.3
- [x] **P1B.6** Local extent index: `ExtentId` → `(DiskId, path, ExtentVersion, checksum, storage class)` — §5.2
- [x] **P1B.7** Orphaned extent cleanup: reclaim uncommitted staged files after safe retention interval — §6.9
- [x] **P1B.8** Startup recovery: restore committed extents, discard incomplete staged files, rebuild local index — §6.10

---

## Phase 2 — Data Node Read/Write Service

Expose the data node's extent storage over the network. Handle write reception, read service, deduplication, and epoch validation.

- [x] **P2.1** Define wire protocol messages for data read/write (protobuf or flatbuffers) — §7
- [x] **P2.2** Protocol version negotiation on connection handshake — §7
- [x] **P2.3** Write reception path: epoch validation → disk eligibility → stage → persist → record op ID → ack — §5.5
- [x] **P2.4** Duplicate operation suppression: record operation identifiers, handle retransmission idempotently — §5.5.5, §4.5.4
- [x] **P2.5** Read service path: locate extent → verify readable state → read range → checksum validation → return — §5.6
- [x] **P2.6** Stale-epoch rejection for writes; conditional stale-epoch reads — §6.5, invariant 4
- [x] **P2.7** Checksum mismatch handling on read: fail read, mark disk/region suspect — §5.6, §6.7
- [x] **P2.8** XFS error handling: detect filesystem errors, transition disk to `degraded`/`failed` — §6.8

---

## Phase 3 — Metadata Service

Implement the strongly consistent replicated state machine that stores cluster membership, placement, volume metadata, and extent mappings.

- [x] **P3.1** Raft consensus integration (e.g., `openraft`) for metadata replication
- [x] **P3.2** Metadata state machine: cluster membership, placement map, volume metadata, extent mappings, protection policies
- [x] **P3.3** Placement epoch: monotonically increasing version, included in all relevant operations — §2.4
- [x] **P3.4** Metadata commit path: validate commit request, apply to state machine, return commit version — §4.5.1 step 8
- [x] **P3.5** Extent mapping commit: volume ID, block range, extent version, epoch, replica locations, checksums, optional CAS — §4.5.2
- [x] **P3.6** Committed state query: lookup extent mapping by operation ID or extent version (for ambiguous write resolution) — §4.9.2
- [x] **P3.7** Crash recovery: restore committed metadata, ordered entry application — §5.7, §6.10
- [x] **P3.8** Quorum partition handling: minority nodes refuse new commits — §6.4, invariant 10

---

## Phase 4 — Client Core

Build the client that serves `ublk` devices, maintains metadata cache and session watermark, and implements the write/read paths.

### 4A — Client Foundation

- [x] **P4A.1** `ublk` device driver integration: register block device, handle kernel read/write requests
- [x] **P4A.2** Client session: stable `SessionId`, per-operation `OperationId` — §4.2
- [x] **P4A.3** Metadata cache: placement epoch, cluster map, volume protection policy, extent mappings — §4.3
- [x] **P4A.4** Session write watermark: monotonically non-decreasing, advanced on commit success — §4.4
- [x] **P4A.5** Metadata freshness checks: watermark-gated cache validation before reads — §4.3, §4.4
- [x] **P4A.6** Stale epoch refresh: stop writes, refresh map, re-resolve, retry — §4.7

### 4B — Replicated Write Path

- [x] **P4B.1** Write pipeline: validate ownership → resolve mapping → compute placement → create extent version → transmit to replicas → await acks → commit metadata → ack to kernel — §4.5.1
- [x] **P4B.2** Durability threshold enforcement: wait for policy-required ack count before commit — §4.5.2
- [x] **P4B.3** Never ack write to `ublk` before metadata commit succeeds — invariant 1
- [x] **P4B.4** Idempotent retry with stable `OperationId` — §4.5.4
- [x] **P4B.5** Partial ack handling: don't commit if insufficient acks; rely on orphan cleanup — §4.9.3

### 4C — Read Path

- [x] **P4C.1** Read pipeline: resolve min visible version → resolve mapping → verify version ≥ watermark → select source → read → checksum verify → return — §4.6
- [x] **P4C.2** Read-your-own-writes enforcement via watermark — invariant 5
- [x] **P4C.3** Replica fallback on source failure — §4.6, §4.9.4
- [x] **P4C.4** Corruption reporting to health subsystem on read failure — §4.9.4

### 4D — Erasure-Coded Paths

- [x] **P4D.1** Reed-Solomon encoder/decoder integration (K data + M parity fragments)
- [x] **P4D.2** EC write path: determine stripe geometry → encode → send fragments to placed nodes → await acks → commit — §4.5.3
- [x] **P4D.3** EC read path: select fragments → decode → verify — §4.6
- [x] **P4D.4** EC reconstruction on fragment failure — §4.6
- [x] **P4D.5** Partial-stripe read-modify-write for sub-stripe overwrites — §4.5.3
- [x] **P4D.6** Adjacent write coalescing to reduce partial-stripe amplification — §4.5.3

---

## Phase 5 — Failure Handling & Recovery

Implement the failure condition requirements from §6 that aren't covered by the happy-path phases above.

- [x] **P5.1** Client crash recovery: uncommitted extents invisible to reads; committed state resolved via metadata — §6.1, invariant 3
- [x] **P5.2** Data node crash after local ack: preserve dedup state or allow metadata interrogation — §6.2
- [x] **P5.3** Data node crash before local durability: never claim success for incomplete writes — §6.3
- [x] **P5.4** Ambiguous write resolution: client queries metadata for operation/extent status before retry — §4.9.2
- [x] **P5.5** Metadata quorum unavailable: block new write acks, allow reads only when policy & watermark permit — §4.9.1
- [x] **P5.6** Disk failure: transition to `failed`, stop IO, report extent set for repair, exclude from placement — §6.6
- [x] **P5.7** Node startup ordering: local recovery → serve committed extents → hide staged files → rejoin metadata — §6.10

---

## Phase 6 — Ownership, Fencing & Security

- [x] **P6.1** Volume ownership / exclusive write lease acquisition and renewal — §4.8
- [x] **P6.2** Fencing: reject writes from clients with expired/revoked leases — §4.8, §8
- [ ] **P6.3** Client authentication to cluster — §8
- [ ] **P6.4** Node-to-node authentication for metadata peers and data replication — §8
- [ ] **P6.5** Per-volume authorization: validate client is authorized before serving IO — §8

---

## Phase 7 — Background Operations

Data node background workers for scrubbing, repair, rebalance, and drain.

- [x] **P7.1** Background scrubbing: verify extent file readability, checksum integrity, local metadata recoverability — §5.12
- [x] **P7.2** Scrub-detected corruption triggers repair workflow — §5.12
- [x] **P7.3** Re-replication: rebuild missing replicas after node/disk failure — §5.13
- [x] **P7.4** Erasure-code rebuild: reconstruct missing fragments — §5.13
- [x] **P7.5** Disk drain: enumerate live extents, report for relocation, serve reads until empty — §5.11
- [x] **P7.6** Capacity rebalance: redistribute extents across nodes for even utilization — §5.13
- [x] **P7.7** Rate limiting / scheduling: background work must not starve foreground IO or metadata — §5.13

---

## Phase 8 — Observability

Instrument every layer with the metrics and health indicators required by §9.

- [x] **P8.1** Per-volume IO success/failure rates
- [x] **P8.2** Client watermark and stale-epoch retry counts
- [x] **P8.3** Per-node foreground and background IO load
- [x] **P8.4** Per-disk health state transitions (with stable disk identifiers) — §9
- [x] **P8.5** Scrub findings
- [x] **P8.6** Repair backlog
- [x] **P8.7** Orphaned extent file counts
- [x] **P8.8** Metadata quorum health and commit latency

---

## Phase 9 — Integration Testing

VM-based, Jepsen-style integration tests per `AGENTS.md` and `TEST_CHECKLIST.md`. 5-node minimum cluster on real VMs with UBLK, XFS, and kernel-level fault injection.

### 9A — Test Infrastructure

- [x] **P9A.1** VM provisioning harness (libvirt/QEMU): create, start, stop, kill, snapshot 5+ VMs with kernel 6.0+
- [x] **P9A.2** Network setup: TAP/bridge networking (not SLIRP) for real inter-node connectivity
- [x] **P9A.3** Fault injection primitives: SIGKILL, SIGSTOP/SIGCONT, iptables partition, asymmetric partition, tc netem delay/loss, dm-delay, dm-flakey, clock skew, full disk
- [x] **P9A.4** Workload generator: wire-protocol-level client with operation log (ack/nack tracking)
- [x] **P9A.5** Consistency checker: verify all acked writes readable after recovery; linearizability validation

### 9B — Consistency Tests

- [x] **P9B.1** Linearizability under `consistency=all` with leader failover
- [x] **P9B.2** Majority-ack consistency: no acknowledged write lost after leader failover
- [x] **P9B.3** Read-your-own-writes with `read-policy=leader` during leader transitions
- [x] **P9B.4** Bounded staleness measurement with `read-policy=any`

### 9C — Availability Tests

- [x] **P9C.1** 1-of-3 node crash: writes continue within election timeout
- [x] **P9C.2** 1-of-5 node crash: zero downtime for unaffected volumes
- [x] **P9C.3** Volume readable during minority partition (from majority side)
- [x] **P9C.4** New leader elected within 2 seconds after leader crash

### 9D — Rebalancing Tests

- [x] **P9D.1** Add node → rebalance → data integrity verified
- [x] **P9D.2** Remove node (drain) → all volumes migrated → no data loss
- [x] **P9D.3** Kill node during rebalance → rebalance resumes after recovery
- [x] **P9D.4** Concurrent client IO during rebalance: no errors

### 9E — Data Integrity Tests

- [x] **P9E.1** Write known pattern → crash all nodes → restart → verify pattern
- [x] **P9E.2** Write during partition → heal → verify convergence (no divergent state)
- [x] **P9E.3** XFS scrub detects injected corruption → heal from healthy replica
- [x] **P9E.4** Snapshot before fault → restore after fault → data matches

### 9F — UBLK Client Tests

- [x] **P9F.1** Mount → write → kill mount process → remount → verify data
- [x] **P9F.2** Mount → partition client from leader → client follows new leader → writes succeed
- [x] **P9F.3** Mount → write through ext4 → crash node → remount → fsck passes

---

## Phase 10 — CLI & Operator Tools

- [ ] **P10.1** `byard` CLI: volume create/delete/list/inspect
- [ ] **P10.2** `byard` CLI: disk list/inspect/drain/remove
- [ ] **P10.3** `byard` CLI: node list/inspect/decommission
- [ ] **P10.4** `byard` CLI: cluster status, placement epoch, quorum health
- [ ] **P10.5** `byard` CLI: mount/unmount volume (ublk attach/detach)

---

## Dependency Graph

```
Phase 0 ──→ Phase 1 ──→ Phase 2 ──→ Phase 4 (client)
                │                        │
                └──→ Phase 3 ────────────┘
                         │
                         ├──→ Phase 5 (failure handling)
                         ├──→ Phase 6 (security/fencing)
                         └──→ Phase 7 (background ops)

Phase 8 (observability) spans all phases — instrument as you build.
Phase 9 (integration tests) depends on phases 1–7.
Phase 10 (CLI) can start after phase 3.
```

---

## Spec Invariant Cross-Reference

| # | Invariant (§10) | Roadmap Items |
|---|----------------|---------------|
| 1 | No write ack before metadata commit | P4B.3 |
| 2 | No knowingly corrupted data returned | P2.5, P2.7 |
| 3 | Uncommitted extents invisible to reads | P1B.4, P1B.7, P1B.8, P5.1 |
| 4 | Stale-epoch writes rejected | P2.6, P4A.6 |
| 5 | Read-your-own-writes via watermark | P4A.4, P4C.2 |
| 6 | No allocations on degraded/draining/failed/removed disks | P1A.5 |
| 7 | Disk failures surfaced to repair logic | P5.6, P7.3 |
| 8 | One XFS per physical disk | P1A.2 |
| 9 | Local durability acks are crash-consistent | P1B.3 |
| 10 | Minority partitions cannot commit | P3.8 |
