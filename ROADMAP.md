# Blockyard Roadmap

Status tracking for the Blockyard distributed block storage system.
Last updated: 2026-04-05

Legend: `[ ]` not started · `[~]` in progress · `[x]` done · `[-]` deferred

---

## Phase 1 — Core (MVP) ✅

Target: single-cluster block storage with Raft replication and UBLK mounting.

### 1.1 Project Foundation
- [x] Cargo workspace with 8 crates
- [x] Shared types, config parsing, error types (`blockyard-common`)
- [x] Example config file matching RFC spec
- [x] CLI skeleton with subcommands (`start`, `volume`, `node`, `mount`, `status`)
- [x] Unit tests across all library crates (413 tests)
- [x] CI pipeline (GitHub Actions: check, clippy, test, fmt)
- [x] Integration test harness (VM-based, Jepsen-style with QEMU/cloud-init)
- [x] VM provisioning scripts (`tests/vm-cluster.sh`) with 5-node cluster

### 1.2 Gossip — Cluster Membership (`blockyard-gossip`)
- [x] SWIM protocol: ping / ping-req / suspect / dead
- [x] UDP transport with message serialization
- [x] Seed-based join (contact seeds → receive membership)
- [x] Piggybacked membership updates on gossip messages
- [x] Incarnation numbers for crashing/restarting nodes
- [x] Configurable probe interval, suspect timeout, probe timeout
- [x] MemberList integration with node state machine
- [x] Transport trait abstraction (UDP + in-memory for tests)
- [x] ZFS health propagation via gossip updates

### 1.3 Multi-Raft Consensus (`blockyard-raft`)
- [x] Multi-Raft group management (create, remove, propose, query state)
- [x] Meta Group: cluster-wide metadata (volume defs, placement map, node inventory)
- [x] Volume Groups: per-volume Raft group lifecycle (create, destroy, membership changes)
- [x] State machine: volume CRUD, placement updates, node register/deregister
- [x] Snapshot and restore for state machine
- [x] Heartbeat consolidation across groups sharing a node pair
- [x] gRPC network transport (`tonic`) for Raft RPCs between nodes
- [x] gRPC server: AppendEntries, InstallSnapshot, RequestVote, ConsolidatedHeartbeat, ForwardProposal, GetState
- [x] gRPC client: RaftNetwork with connection pooling and peer management
- [x] Leader election and automatic failover (in-process via MultiRaft propose)
- [x] Voter/learner management for Meta Group (via gRPC AddEntries)
- [ ] Raft log storage on dedicated ZFS dataset (`blockyard/raft-log`) — deferred to production ZFS integration
- [ ] Raft snapshot via ZFS snapshot — deferred to production ZFS integration

### 1.4 Storage Backend (`blockyard-storage`)
- [x] StorageBackend trait abstraction
- [x] ZFS zvol create / destroy / resize via CLI shelling (`ZfsBackend`)
- [x] MemoryBackend for testing (no ZFS required)
- [x] Zvol naming convention: `<pool>/vol-<volume-id>`
- [x] Pool capacity reporting (total / used / available)
- [x] Placement engine: filter → spread → balance → prefer
- [x] Failure domain constraint satisfaction
- [x] ZFS pool health monitoring (`zpool status`, `zpool list`) on periodic interval
- [x] Detect and report: degraded vdevs, faulted disks, checksum errors, scrub errors
- [x] Node-local health state machine: healthy → degraded → faulted (based on ZFS pool state)
- [x] Propagate ZFS health status via gossip (node tags / health metadata)
- [x] Placement engine excludes nodes with faulted pools from new volume placement
- [x] Trigger automatic re-replication when a node's pool is persistently degraded
- [x] Extent-level addressing within zvols (4MB default)

### 1.5 Block Replication (`blockyard-protocol`)
- [x] Binary wire protocol: request/response framing (33B request header, 13B response header)
- [x] Op types: READ, WRITE, FLUSH, TRIM
- [x] Request pipelining (multiple in-flight requests via codec)
- [x] TCP connection pooling
- [x] Tokio codec (Encoder/Decoder) for async framed I/O
- [x] Protocol server with RequestHandler trait and TCP e2e path
- [x] Write path: client → server → handler → Raft propose → response
- [x] Read path: client → server → handler → state machine read → response

### 1.6 Volume Mounting (`blockyard-ublk`)
- [x] Mount/unmount abstraction with UBLK and NBD backends
- [x] Cluster client: discover volume leader, follow failovers
- [x] UBLK server with multi-queue I/O configuration
- [x] `/dev/ublkbN` block device path management
- [x] Device recovery (recover reclaims existing device)
- [x] Write batching with configurable alignment (4KB) and max delay (1ms)
- [x] io_uring UBLK bindings: ioctl constants, ctrl/io command structs, queue config
- [x] Kernel module loading (`modprobe ublk_drv`)

### 1.7 CLI & Control Plane
- [x] `blockyard start` — full node startup with gRPC server, protocol server, gossip, health monitor, heartbeat generator
- [x] `blockyard volume create/delete/list/status/resize` — wired to Meta Raft
- [x] `blockyard mount <volume>` — UBLK/NBD client
- [x] `blockyard status` — cluster health summary
- [x] `blockyard node list` — node inventory table
- [x] `blockyard node status <name>` — node view with ZFS health info

### 1.8 Integration Testing
- [x] 5-node QEMU VM cluster with cloud-init provisioning
- [x] VM lifecycle management (provision, start, stop, SSH, deploy)
- [x] Fault injection: SIGKILL, SIGSTOP/SIGCONT, iptables, tc netem, dm-delay, dm-flakey, clock skew
- [x] Workload generator with write/read logs and P99 latency tracking
- [x] Post-condition checker: write durability, read consistency, ZFS integrity, cluster health
- [x] Test scenarios: consistency (linearizability, majority-ack), availability (crash survival, leader election), integrity (crash-restart-verify, partition-heal)
- [x] Verified: fault injection works on real 5-node cluster (node crash → others survive)

---

## Phase 2 — Production Readiness

Target: operational maturity for real workloads.

### 2.1 Rebalancing
- [ ] Detect capacity imbalance (configurable threshold, default 20%)
- [ ] Compute new placement map via placement engine
- [ ] Add target node as Raft learner
- [ ] Bulk data transfer via ZFS send/receive
- [ ] Promote learner → voter, remove old replica
- [ ] Throttle: max concurrent moves per node, bandwidth cap
- [ ] Rebalance status reporting in CLI/metrics

### 2.2 Online Operations
- [ ] Online volume expansion (`zfs set volsize`, UBLK resize notification)
- [ ] Node drain (`blockyard node drain`) — migrate all volumes, then remove
- [ ] Change replication factor (`blockyard volume set --replicas N`)
- [ ] Change consistency mode at runtime

### 2.3 Per-Volume Tuning
- [ ] Write consistency modes: `all` / `majority` / `single`
- [ ] Read policies: `leader` / `any` / `local`
- [ ] Per-volume affinity and anti-affinity enforcement
- [ ] Per-volume failure domain spreading

### 2.4 Security
- [ ] Mutual TLS for all node-to-node communication
- [ ] Mutual TLS for client-to-cluster communication
- [ ] Certificate generation and rotation
- [ ] Token-based authentication (pre-shared bearer tokens)
- [ ] Volume-level ACLs (read-only, read-write per client)

### 2.5 Observability
- [ ] Prometheus `/metrics` endpoint on each node
- [ ] Cluster metrics: nodes total by state
- [ ] Per-volume metrics: IOPS, throughput, latency histograms
- [ ] Per-node metrics: ZFS capacity, Raft group count, leader count
- [ ] ZFS health metrics
- [ ] Cluster-wide ZFS health summary
- [ ] Rebalance progress metrics

### 2.6 Snapshots
- [ ] Volume snapshots delegated to ZFS (`zfs snapshot`)
- [ ] Snapshot list/delete via CLI
- [ ] Consistent snapshots across replicas (Raft barrier)

---

## Phase 3 — Advanced Features

Target: WAN, large-scale, and ecosystem integration.

### 3.1 Networking
- [ ] QUIC transport for WAN / cross-datacenter deployments
- [ ] NBD fallback for kernels < 6.0

### 3.2 Scalability
- [ ] Volume striping across multiple node sets (stripe groups)
- [ ] Erasure coding (k+m via Reed-Solomon)
- [ ] Client write-back cache

### 3.3 Data Management
- [ ] Volume cloning via ZFS clone
- [ ] Cross-cluster replication (async DR)

### 3.4 Ecosystem
- [ ] REST/gRPC API for orchestrator integration
- [ ] Kubernetes CSI driver
- [ ] `libblockyard` client library for direct application integration

---

## Open Questions (from RFC)

| # | Question | Status |
|---|----------|--------|
| 1 | Raft log storage: dedicated ZFS dataset vs. zvol metadata? | Leaning dedicated dataset |
| 2 | Extent-level vs. volume-level Raft groups? | Volume-level for MVP |
| 3 | Should Blockyard manage ZFS pool creation? | No — operator pre-creates |
| 4 | `libblockyard` for direct app integration? | Deferred to Phase 3 |
| 5 | Log stale reads during partition heal? | Undecided |
| 6 | Formal maximum cluster size? | Undecided |
