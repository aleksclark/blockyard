# Blockyard Roadmap

Status tracking for the Blockyard distributed block storage system.
Last updated: 2026-04-04

Legend: `[ ]` not started · `[~]` in progress · `[x]` done · `[-]` deferred

---

## Phase 1 — Core (MVP)

Target: single-cluster block storage with Raft replication and UBLK mounting.

### 1.1 Project Foundation
- [x] Cargo workspace with crate layout
- [x] Shared types, config parsing, error types (`blockyard-common`)
- [x] Example config file matching RFC spec
- [x] CLI skeleton with subcommands (`start`, `volume`, `node`, `mount`, `status`)
- [ ] CI pipeline (cargo check, clippy, test, fmt)
- [ ] Integration test harness (multi-node in-process)

### 1.2 Gossip — Cluster Membership (`blockyard-gossip`)
- [ ] SWIM protocol: ping / ping-req / suspect / dead
- [ ] UDP transport with message serialization
- [ ] Seed-based join (contact seeds → receive membership)
- [ ] Piggybacked membership updates on gossip messages
- [ ] Incarnation numbers for crashing/restarting nodes
- [ ] Configurable probe interval, suspect timeout, probe timeout
- [ ] MemberList integration with node state machine

### 1.3 Multi-Raft Consensus (`blockyard-raft`)
- [ ] Integrate `openraft` as Raft engine
- [ ] Meta Group: cluster-wide metadata (volume defs, placement map, node inventory)
- [ ] Volume Groups: per-volume Raft group lifecycle (create, destroy, membership changes)
- [ ] Heartbeat consolidation across groups sharing a node pair
- [ ] Raft log storage on dedicated ZFS dataset (`blockyard/raft-log`)
- [ ] Raft snapshot via ZFS snapshot
- [ ] Leader election and automatic failover
- [ ] Voter/learner management for Meta Group

### 1.4 Storage Backend (`blockyard-storage`)
- [ ] ZFS zvol create / destroy / resize via `libzfs` (or CLI shelling)
- [ ] Zvol naming convention: `<pool>/vol-<volume-id>`
- [ ] Pool capacity reporting (total / used / available)
- [ ] Extent-level addressing within zvols (4MB default)
- [ ] Placement engine: filter → spread → balance → prefer
- [ ] Failure domain constraint satisfaction

### 1.5 Block Replication (`blockyard-protocol`)
- [ ] Binary wire protocol: request/response framing
- [ ] Op types: READ, WRITE, FLUSH, TRIM
- [ ] Request pipelining (multiple in-flight requests)
- [ ] TCP transport with connection pooling
- [ ] Write path: client → leader → Raft propose → replicate → ack
- [ ] Read path: client → any replica (based on read-policy)

### 1.6 Volume Mounting (`blockyard-ublk`)
- [ ] UBLK server using io_uring (Linux 6.0+)
- [ ] `/dev/ublkbN` block device creation
- [ ] Multi-queue I/O (one ring per CPU core)
- [ ] Cluster client: discover volume leader, follow failovers
- [ ] Write batching and alignment (4KB, 1ms max delay)
- [ ] Device recovery on mount process restart

### 1.7 CLI & Control Plane
- [ ] `blockyard start` — full node startup with config
- [ ] `blockyard volume create/delete/list/status` — via Meta Raft
- [ ] `blockyard mount <volume>` — UBLK client
- [ ] `blockyard status` — cluster health summary
- [ ] `blockyard node list` — node inventory table

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
- [ ] Rebalance progress metrics
- [ ] `blockyard volume status <name>` — detailed per-volume view

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
