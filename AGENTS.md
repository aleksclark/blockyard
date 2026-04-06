# AGENTS.md — Blockyard

Instructions for AI agents and human contributors working on this codebase.

## Project Overview

Blockyard is a distributed block-level storage system written in Rust. It prioritizes CA (Consistency + Availability), runs a single process per node, delegates local storage to ZFS zvols, uses Multi-Raft for consensus, exposes volumes via UBLK, and discovers peers through gossip.

Read these before starting any work:
- [`README.md`](README.md) — architecture and quick start
- [`ROADMAP.md`](ROADMAP.md) — current status of every deliverable
- [`config/blockyard.example.toml`](config/blockyard.example.toml) — full configuration reference

## Workspace Layout

```
crates/
├── blockyard/            # Main binary — node process, CLI entry point
├── blockyard-cli/        # Lightweight remote client binary (byard)
├── blockyard-common/     # Shared types, config, errors — depended on by all other crates
├── blockyard-gossip/     # SWIM gossip protocol, MemberList, failure detection
├── blockyard-protocol/   # Client↔cluster binary wire protocol
├── blockyard-raft/       # Multi-Raft engine (openraft), Meta Group, Volume Groups
├── blockyard-storage/    # ZFS zvol backend, placement engine
└── blockyard-ublk/       # UBLK (io_uring) and NBD volume mounting
```

Dependency direction: `blockyard-common` is the leaf. All other library crates depend on it. The `blockyard` binary crate depends on all library crates. `blockyard-cli` depends only on `blockyard-common`.

## Build & Run

```bash
cargo build                     # debug build
cargo build --release           # release build (LTO, single codegen unit)
cargo check                     # type-check without codegen
cargo clippy --workspace        # lint — must pass with zero warnings
cargo fmt --all -- --check      # format check
```

Minimum toolchain: Rust 1.85+ (edition 2024).

## Code Conventions

### Style
- Follow existing patterns in the crate you are modifying.
- Use `thiserror` for library error types. Use `anyhow` only in binary crates.
- Async runtime is **tokio**. Do not introduce alternatives.
- Use `tracing` for all logging (`info!`, `debug!`, `warn!`, `error!`). Never use `println!` in library crates.
- Use `parking_lot` over `std::sync` for mutexes and rwlocks.
- Prefer `bytes::Bytes` for owned byte buffers in protocol paths.
- All public types must derive `Debug`. Derive `Clone`, `Serialize`, `Deserialize` where appropriate.
- No `unwrap()` or `expect()` in library code except for cases that are provably infallible (document why with a comment).

### Naming
- Crate names: `blockyard-<subsystem>`
- Type aliases for IDs: `NodeId`, `VolumeId`, `ExtentId`, `RaftGroupId` (defined in `blockyard-common::types`)
- Config structs: `<Thing>Section` (e.g., `RaftSection`, `GossipSection`)
- Error variants: `Error::<Category>(String)` or `Error::<Category>(#[from] SourceError)`

### Module Structure
- Each crate has a `lib.rs` that re-exports the primary public API.
- Internal modules are private unless there is a reason to expose them.
- Keep files focused. One major struct or trait per file is preferred.

## Testing Requirements

### Unit Tests — 95% Line Coverage Required

Every library crate (`blockyard-common`, `blockyard-gossip`, `blockyard-raft`, `blockyard-storage`, `blockyard-protocol`, `blockyard-ublk`) must maintain **≥95% line coverage** in unit tests.

Measure coverage with:
```bash
cargo llvm-cov --workspace --lib --lcov --output-path lcov.info
cargo llvm-cov report --workspace --lib
```

Rules:
- Unit tests live in a `#[cfg(test)] mod tests` block at the bottom of the file they test.
- Every public function and method must have at least one test.
- Every enum variant must be exercised in at least one test.
- Every error path must be tested. If a function returns `Result`, test both `Ok` and `Err` cases.
- Test names: `test_<function_name>_<scenario>` (e.g., `test_place_volume_respects_affinity`, `test_member_list_mark_state_unknown_node`).
- Use `#[tokio::test]` for async tests.
- No `#[ignore]` without a tracking issue comment.
- Mocks and fakes: create them in a `testutil` module within the crate (e.g., `src/testutil.rs`), gated behind `#[cfg(test)]`. Prefer hand-written fakes over mocking frameworks.
- For `blockyard-storage`: the ZFS backend must be tested against a trait abstraction so that unit tests run without a real ZFS pool. Define a `StorageBackend` trait and implement it for both `ZfsBackend` (production) and `MemoryBackend` (tests).
- For `blockyard-gossip`: the UDP transport must be abstracted behind a trait so unit tests can use in-memory channels.
- For `blockyard-raft`: use `openraft`'s in-memory network and storage implementations for unit tests. Do not require a running cluster.

### Integration Tests — VM-Based, Jepsen-Style

**Docker is not acceptable for integration tests.** Blockyard depends on ZFS (kernel module), UBLK (kernel 6.0+), and real block device semantics. Docker containers share the host kernel and cannot properly isolate these. All integration tests must run in real virtual machines.

#### Infrastructure

Tests run against a cluster of VMs managed by an automation tool (e.g., libvirt/QEMU via `testinfra`, Vagrant, or a custom Rust harness). Each VM:
- Runs its own Linux kernel (6.0+ for UBLK tests)
- Has its own ZFS pool on a virtual disk
- Runs a single `blockyard` process
- Is independently controllable (start, stop, kill, network partition, disk fault)

Minimum cluster size for integration tests: **5 nodes** (allows 2-node failures with 3-replica quorum).

#### Fault Injection (Jepsen-style)

Integration tests must systematically verify correctness under failures. The test harness must support these fault injection primitives:

| Fault | Implementation |
|-------|----------------|
| **Node crash** | `SIGKILL` the blockyard process (not graceful shutdown) |
| **Node pause** | `SIGSTOP` / `SIGCONT` to simulate GC pauses or freezes |
| **Network partition** | `iptables` rules to drop/reject traffic between specific node pairs |
| **Asymmetric partition** | Node A can reach B, but B cannot reach A |
| **Network delay** | `tc netem` to add latency (e.g., 100ms, 500ms) |
| **Network packet loss** | `tc netem` with configurable loss percentage |
| **Disk slow** | `dm-delay` or cgroup I/O throttling on the ZFS pool's backing device |
| **Disk fault** | `dm-flakey` to inject transient I/O errors |
| **Clock skew** | Adjust VM system clock forward/backward |
| **Full disk** | Fill the ZFS pool to capacity |

#### Required Test Scenarios

Each scenario runs concurrent client workloads while injecting faults, then verifies invariants.

**Consistency tests:**
- Linearizability of writes under `consistency=all` with leader failover
- Majority-ack consistency: no acknowledged write is lost after leader failover
- Single-ack consistency: leader crash may lose unacknowledged writes; verify replicated writes survive
- No stale reads with `read-policy=leader` during leader transitions
- Bounded staleness with `read-policy=any` — quantify and assert a bound

**Availability tests:**
- Cluster survives 1-of-3 node crash (writes continue within Raft election timeout)
- Cluster survives 1-of-5 node crash with zero downtime for unaffected volumes
- Volume remains readable during minority partition (from majority side)
- New leader elected within 2 seconds after leader crash

**Rebalancing tests (Phase 2+):**
- Add node → volumes rebalance → data integrity preserved
- Remove node (drain) → all volumes migrated → no data loss
- Kill node during rebalancing → rebalance resumes after recovery
- Concurrent client I/O during rebalance has no errors

**Data integrity tests:**
- Write known pattern → crash all nodes → restart → verify pattern
- Write during network partition → heal partition → verify convergence (no divergent state)
- ZFS scrub detects injected corruption → Blockyard heals from healthy replica
- Snapshot before fault → restore after fault → data matches snapshot

**UBLK client tests:**
- Mount → write → kill mount process → remount → verify data
- Mount → partition client from leader → client follows new leader → writes succeed
- Mount → write through ext4 → crash node → remount → fsck passes

#### Verification

All integration tests must use a **checker** that validates post-conditions:

1. **Write log**: the client records every attempted write (offset, data, ack status). After the test, read back every acknowledged offset and verify the data matches.
2. **Raft log consistency**: dump Raft logs from all surviving nodes. Verify no committed entry is missing from any replica that was in the group at commit time.
3. **ZFS integrity**: run `zpool scrub` and `zpool status` on every surviving node. Zero checksum errors.
4. **No zombie state**: after cluster recovery, all volumes report `Healthy` (or `Degraded` with correct replica count).

#### Running Integration Tests

Integration tests live in a top-level `tests/` directory (not inside any crate):
```
tests/
├── harness/          # VM provisioning, fault injection, client workload generators
│   ├── cluster.rs    # Cluster lifecycle (create, destroy, per-node control)
│   ├── faults.rs     # Fault injection primitives
│   ├── workload.rs   # Read/write workload generators
│   └── checker.rs    # Post-condition verification
├── consistency/      # Linearizability, write durability, read staleness
├── availability/     # Failover timing, quorum behavior
├── rebalance/        # Data migration under faults
├── integrity/        # Data corruption, recovery, snapshots
└── ublk/             # Client mount/unmount, failover, filesystem
```

```bash
# Run the full integration suite (provisions VMs, slow)
cargo test --test '*' -- --test-threads=1

# Run a specific scenario
cargo test --test consistency -- linearizable_writes_during_leader_failover
```

Integration tests are expected to be slow (minutes per scenario). They run in CI on dedicated hardware, not on every PR. PRs must pass unit tests and clippy; integration tests run on merge to `main`.

## PR Checklist

Before submitting work:
1. `cargo fmt --all` — code is formatted
2. `cargo clippy --workspace` — zero warnings
3. `cargo test --workspace` — all unit tests pass
4. Coverage ≥95% for every library crate modified
5. New public API has doc comments
6. `ROADMAP.md` updated if a task is completed
7. No `TODO` without a tracking issue reference
8. No `unsafe` without a `// SAFETY:` comment explaining the invariant

## Bug Fix Rule

**Every bug fix MUST begin with a failing test that reproduces the bug.** Do not read the source code or attempt a fix until the test exists and fails. The workflow is:

1. Write an integration test (preferred) or unit test that exercises the exact failing behavior reported
2. Run the test and confirm it fails for the expected reason
3. Only then investigate the source code and implement the fix
4. Run the test again and confirm it passes
5. Run the full test suite to confirm no regressions

This applies to all bugs — whether found by users, CI, or during development. No exceptions.
