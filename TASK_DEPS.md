# Task Dependency Map

Direct dependencies for every task in `ROADMAP.md`. A task may begin only after all listed dependencies are complete.

`—` = no dependencies (root task).

---

## Phase 0 — Project Skeleton & Shared Types

| Task | Description | Depends On |
|------|-------------|------------|
| P0.1 | Cargo workspace and crate scaffolding | — |
| P0.2 | Core ID types | P0.1 |
| P0.3 | `ProtectionPolicy` enum | P0.1 |
| P0.4 | `DiskState` enum | P0.1 |
| P0.5 | Shared error types | P0.1 |
| P0.6 | Config structs | P0.1, P0.2 |
| P0.7 | Tracing subscriber init | P0.1 |
| P0.8 | CI pipeline | P0.1 |

---

## Phase 1A — Disk Inventory & Health

| Task | Description | Depends On |
|------|-------------|------------|
| P1A.1 | Disk discovery and persistent `DiskId` | P0.2, P0.5, P0.6 |
| P1A.2 | XFS filesystem validation per disk | P1A.1 |
| P1A.3 | `DiskState` machine and transition rules | P0.4, P1A.1 |
| P1A.4 | Per-disk telemetry collection | P1A.1 |
| P1A.5 | Allocation guards (refuse extents on bad disks) | P1A.3 |
| P1A.6 | Bad-region map | P1A.1, P1A.4 |
| P1A.7 | Disk qualification state (burn-in) | P1A.1, P1A.3 |

---

## Phase 1B — Extent File Lifecycle

| Task | Description | Depends On |
|------|-------------|------------|
| P1B.1 | Extent file path layout on XFS | P0.2, P1A.1 |
| P1B.2 | Write staging (temp file + checksum) | P1B.1, P1A.5 |
| P1B.3 | Local durability (fsync guarantees) | P1B.2 |
| P1B.4 | Atomic promotion (staged → committed) | P1B.3 |
| P1B.5 | Immutability enforcement | P1B.4 |
| P1B.6 | Local extent index | P1B.4 |
| P1B.7 | Orphaned extent cleanup | P1B.6 |
| P1B.8 | Startup recovery | P1B.6, P1B.7 |

---

## Phase 2 — Data Node Read/Write Service

| Task | Description | Depends On |
|------|-------------|------------|
| P2.1 | Wire protocol message definitions | P0.2, P0.3, P0.5 |
| P2.2 | Protocol version negotiation | P2.1 |
| P2.3 | Write reception path | P2.2, P1B.4, P1A.5 |
| P2.4 | Duplicate operation suppression | P2.3 |
| P2.5 | Read service path | P2.2, P1B.6 |
| P2.6 | Stale-epoch rejection | P2.3, P2.5, P3.3 |
| P2.7 | Checksum mismatch handling on read | P2.5, P1A.3, P1A.6 |
| P2.8 | XFS error handling | P2.3, P2.5, P1A.3 |

---

## Phase 3 — Metadata Service

| Task | Description | Depends On |
|------|-------------|------------|
| P3.1 | Raft consensus integration | P0.1, P0.2, P0.5 |
| P3.2 | Metadata state machine | P3.1, P0.3 |
| P3.3 | Placement epoch | P3.2 |
| P3.4 | Metadata commit path | P3.2, P3.3 |
| P3.5 | Extent mapping commit | P3.4 |
| P3.6 | Committed state query | P3.5 |
| P3.7 | Crash recovery | P3.2 |
| P3.8 | Quorum partition handling | P3.1, P3.4 |

---

## Phase 4A — Client Foundation

| Task | Description | Depends On |
|------|-------------|------------|
| P4A.1 | `ublk` device driver integration | P0.1 |
| P4A.2 | Client session (`SessionId`, `OperationId`) | P0.2 |
| P4A.3 | Metadata cache | P2.1, P3.3 |
| P4A.4 | Session write watermark | P4A.2 |
| P4A.5 | Metadata freshness checks | P4A.3, P4A.4 |
| P4A.6 | Stale epoch refresh | P4A.3, P3.3 |

---

## Phase 4B — Replicated Write Path

| Task | Description | Depends On |
|------|-------------|------------|
| P4B.1 | Write pipeline (end-to-end) | P4A.1, P4A.2, P4A.3, P4A.4, P2.3, P3.4 |
| P4B.2 | Durability threshold enforcement | P4B.1, P0.3 |
| P4B.3 | No ack before metadata commit | P4B.1, P3.4 |
| P4B.4 | Idempotent retry with `OperationId` | P4B.1, P2.4 |
| P4B.5 | Partial ack handling | P4B.1, P1B.7 |

---

## Phase 4C — Read Path

| Task | Description | Depends On |
|------|-------------|------------|
| P4C.1 | Read pipeline (end-to-end) | P4A.1, P4A.3, P4A.5, P2.5, P3.6 |
| P4C.2 | Read-your-own-writes enforcement | P4C.1, P4A.4 |
| P4C.3 | Replica fallback on source failure | P4C.1 |
| P4C.4 | Corruption reporting to health subsystem | P4C.3, P1A.3 |

---

## Phase 4D — Erasure-Coded Paths

| Task | Description | Depends On |
|------|-------------|------------|
| P4D.1 | Reed-Solomon encoder/decoder | P0.3 |
| P4D.2 | EC write path | P4B.1, P4D.1 |
| P4D.3 | EC read path | P4C.1, P4D.1 |
| P4D.4 | EC reconstruction on fragment failure | P4D.3 |
| P4D.5 | Partial-stripe read-modify-write | P4D.2, P4D.3 |
| P4D.6 | Adjacent write coalescing | P4D.2 |

---

## Phase 5 — Failure Handling & Recovery

| Task | Description | Depends On |
|------|-------------|------------|
| P5.1 | Client crash recovery | P4B.1, P4C.1, P3.6, P1B.7 |
| P5.2 | Data node crash after local ack | P2.3, P2.4, P1B.8 |
| P5.3 | Data node crash before local durability | P1B.3, P1B.8 |
| P5.4 | Ambiguous write resolution | P4B.1, P3.6 |
| P5.5 | Metadata quorum unavailable | P4B.1, P4C.1, P3.8 |
| P5.6 | Disk failure handling | P1A.3, P1B.6, P3.2 |
| P5.7 | Node startup ordering | P1B.8, P3.7 |

---

## Phase 6 — Ownership, Fencing & Security

| Task | Description | Depends On |
|------|-------------|------------|
| P6.1 | Volume ownership / write lease | P3.2, P4A.2 |
| P6.2 | Fencing (reject unauthorized writes) | P6.1, P2.3 |
| P6.3 | Client authentication | P2.2 |
| P6.4 | Node-to-node authentication | P2.2, P3.1 |
| P6.5 | Per-volume authorization | P6.3, P6.4, P3.2 |

---

## Phase 7 — Background Operations

| Task | Description | Depends On |
|------|-------------|------------|
| P7.1 | Background scrubbing | P1B.6, P1A.4 |
| P7.2 | Scrub-detected corruption → repair | P7.1, P1A.3, P3.2 |
| P7.3 | Re-replication | P2.3, P2.5, P3.5, P5.6 |
| P7.4 | Erasure-code rebuild | P7.3, P4D.1 |
| P7.5 | Disk drain | P1A.3, P1B.6, P7.3 |
| P7.6 | Capacity rebalance | P7.3, P3.3 |
| P7.7 | Rate limiting / scheduling | P7.1, P7.3 |

---

## Phase 8 — Observability

| Task | Description | Depends On |
|------|-------------|------------|
| P8.1 | Per-volume IO success/failure rates | P4B.1, P4C.1 |
| P8.2 | Client watermark and stale-epoch retry counts | P4A.4, P4A.6 |
| P8.3 | Per-node foreground and background IO load | P2.3, P2.5, P7.7 |
| P8.4 | Per-disk health state transitions | P1A.3, P1A.4 |
| P8.5 | Scrub findings | P7.1 |
| P8.6 | Repair backlog | P7.3 |
| P8.7 | Orphaned extent file counts | P1B.7 |
| P8.8 | Metadata quorum health and commit latency | P3.4 |

---

## Phase 9A — Test Infrastructure

| Task | Description | Depends On |
|------|-------------|------------|
| P9A.1 | VM provisioning harness | — |
| P9A.2 | TAP/bridge networking | P9A.1 |
| P9A.3 | Fault injection primitives | P9A.1, P9A.2 |
| P9A.4 | Workload generator | P2.1, P9A.1 |
| P9A.5 | Consistency checker | P9A.4 |

---

## Phase 9B — Consistency Tests

| Task | Description | Depends On |
|------|-------------|------------|
| P9B.1 | Linearizability under `consistency=all` | P9A.3, P9A.5, P4B.1, P3.4 |
| P9B.2 | Majority-ack: no lost acknowledged writes | P9B.1 |
| P9B.3 | Read-your-own-writes during leader transitions | P9A.5, P4C.2 |
| P9B.4 | Bounded staleness measurement | P9A.5, P4C.1 |

---

## Phase 9C — Availability Tests

| Task | Description | Depends On |
|------|-------------|------------|
| P9C.1 | 1-of-3 node crash: writes continue | P9A.3, P9A.4, P4B.1, P3.8 |
| P9C.2 | 1-of-5 node crash: zero downtime for unaffected volumes | P9C.1 |
| P9C.3 | Volume readable during minority partition | P9A.3, P4C.1, P3.8 |
| P9C.4 | New leader elected within 2 seconds | P9A.3, P3.1 |

---

## Phase 9D — Rebalancing Tests

| Task | Description | Depends On |
|------|-------------|------------|
| P9D.1 | Add node → rebalance → data integrity | P9A.5, P7.6 |
| P9D.2 | Remove node (drain) → migrate → no data loss | P9A.5, P7.5 |
| P9D.3 | Kill node during rebalance → resume | P9A.3, P9D.1 |
| P9D.4 | Concurrent IO during rebalance | P9A.4, P9D.1 |

---

## Phase 9E — Data Integrity Tests

| Task | Description | Depends On |
|------|-------------|------------|
| P9E.1 | Write → crash all → restart → verify | P9A.3, P9A.5, P5.7 |
| P9E.2 | Write during partition → heal → verify convergence | P9A.3, P9A.5, P3.8 |
| P9E.3 | XFS scrub detects corruption → heal from replica | P9A.3, P7.2, P7.3 |
| P9E.4 | Snapshot before fault → restore → verify | P9A.3, P9A.5 |

---

## Phase 9F — UBLK Client Tests

| Task | Description | Depends On |
|------|-------------|------------|
| P9F.1 | Mount → write → kill → remount → verify | P9A.1, P4A.1, P4B.1, P4C.1 |
| P9F.2 | Partition client from leader → follow new leader | P9A.3, P4A.6, P3.8 |
| P9F.3 | Write through ext4 → crash → remount → fsck | P9F.1, P9A.3 |

---

## Phase 10 — CLI & Operator Tools

| Task | Description | Depends On |
|------|-------------|------------|
| P10.1 | Volume create/delete/list/inspect | P3.2, P0.6 |
| P10.2 | Disk list/inspect/drain/remove | P1A.1, P1A.3, P7.5 |
| P10.3 | Node list/inspect/decommission | P3.2 |
| P10.4 | Cluster status, placement epoch, quorum health | P3.3, P3.8 |
| P10.5 | Mount/unmount volume (ublk attach/detach) | P4A.1, P10.1 |

---

## Critical Path

The longest dependency chain determines minimum calendar time:

```
P0.1 → P0.2 → P1A.1 → P1B.1 → P1B.2 → P1B.3 → P1B.4 → P1B.6
  → P2.5 → P4C.1 → P4C.2 → P9B.3
                                          (parallel)
P0.1 → P3.1 → P3.2 → P3.4 → P3.5 → P3.6
  → P4B.1 → P4B.3 → P9B.1 → P9B.2
```

Both converge at the integration test phase. The metadata service (Phase 3) and local storage (Phases 1–2) are independent of each other and should be developed in parallel.
