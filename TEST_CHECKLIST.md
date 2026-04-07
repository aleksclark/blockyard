# Integration Test Validation Checklist

A checklist for verifying that a distributed block storage system's integration
test suite provides genuine validation — not tautological, not vacuously
passing, not undermined by OS caching or timing assumptions.

Rust-specific where noted. Applicable to any system using Raft consensus,
erasure coding, UBLK/NBD block devices, ZFS backends, and VM-based test
infrastructure.

---

## 1. Compilation & Gating

- [ ] **All test crates compile.** Run `cargo test --no-run` for every test
      target. Uncommitted working tree changes from automated agents frequently
      introduce unresolved imports.
- [ ] **Gate mechanism works both ways.** If tests are `#[ignore]` + env-gated
      (e.g. `BLOCKYARD_INTEGRATION=1`), verify that:
  - Tests DO run when both conditions are met
  - Tests DO NOT run in normal `cargo test`
  - CI configuration actually sets the env var and passes `--ignored`
- [ ] **Every `#[ignore]` has a tracking comment** explaining why and when it
      will be un-ignored (per project conventions).

---

## 2. Infrastructure Authenticity

### Block Device Stack
- [ ] Tests use real kernel block devices (UBLK/NBD), not userspace fakes
- [ ] Filesystem is mounted on the actual block device (`/dev/ublkbN`), not a
      tmpfs or loopback
- [ ] ZFS pools are ONLINE on storage nodes (verify with `zpool list`, not just
      process presence)
- [ ] Storage nodes actually USE the ZFS backend — not silently falling back to
      in-memory block store. Verify by checking zvol existence or logs.

### Networking
- [ ] VMs have real inter-node networking, not just loopback with port
      forwarding. SLIRP/user-mode networking makes all nodes appear at
      `127.0.0.1` — iptables-based partition tests are broken in this mode.
- [ ] If using port-forwarded networking: document the limitation. Network
      partition tests require TAP/bridge networking or proxy-based partitioning.
- [ ] Partition tests actually isolate the intended node pair, not all traffic
      (including the test harness's SSH).

### Fault Injection
- [ ] Process kills use `SIGKILL`, not `SIGTERM` — a clean shutdown is not a
      crash.
- [ ] Network partitions sever TCP at the transport level (kill proxy, iptables
      DROP, tc loss 100%), not application-level disconnects.
- [ ] Disk faults use kernel primitives (`dm-flakey`, `dm-delay`), not
      application-level error injection.
- [ ] Clock skew uses real `date -s` or `adjtimex`, not mocked clocks.
- [ ] VM-level crashes (`qemu-system-x86_64 -monitor quit`) exist for true
      power-loss testing, not just process kills (which still allow OS buffer
      flush).

---

## 3. Assertion Quality

### No Vacuous Passes
- [ ] **Every test has at least one assertion that can fail.** Search for:
  - `let _ = verify(...)` — discards verification result
  - `assert!(x.is_ok() || x.is_err())` — tautologically true
  - Tests with zero `assert!` / `assert_eq!` / `.unwrap()` after the action
  - Tests that only print output without checking it
- [ ] **Error-path tests assert the error**, not just that something happened.
      `assert!(result.is_err())` is better than no assertion, but
      `assert!(matches!(result, Err(SpecificError::_)))` is better still.

### Verification Strength
- [ ] Data integrity uses cryptographic hashes (SHA-256, not just file size or
      existence checks)
- [ ] Checksums are computed BEFORE the fault and verified AFTER recovery — not
      computed and verified in the same breath
- [ ] Stress/workload tests check the tool's EXIT CODE, not just that it ran.
      A stress binary that exits 0 on corruption is worse than no test.
- [ ] Concurrent I/O tests verify data correctness, not just that files exist
      (`file_count > 0` proves nothing about integrity)

### Minimum Thresholds
- [ ] Workload-based tests assert a minimum number of completed operations
      proportional to runtime. A 15-second workload that acks 0 writes should
      not pass.
- [ ] Checker/validator logic does not pass vacuously when the workload
      generated no operations (e.g. `!acked.is_empty() || write_count == 0`
      passes with zero writes).

---

## 4. Page Cache & Caching Invalidation

This is the single most common way distributed storage tests lie. Linux page
cache serves reads from RAM, bypassing the entire storage stack.

- [ ] **Any test that writes and reads on the same mount MUST invalidate page
      cache** between write and verify. Options:
  - `echo 3 > /proc/sys/vm/drop_caches` on the client
  - Unmount → remount between write and read
  - Use `O_DIRECT` for verification reads
  - Read from a DIFFERENT client that never cached the data
- [ ] Tests that crash nodes and verify from the SAME mounted filesystem are
      suspect — the client's page cache may serve stale data even though the
      backend lost it.
- [ ] The strongest durability tests: crash → remount (without mkfs) → verify.
      This forces a cold read from the block device.

---

## 5. Consensus & Leader Behavior

- [ ] **Leader identity is verified, not assumed.** Tests that claim to "crash
      the leader" must query the cluster to find the actual leader, not assume
      node 0 is always leader.
- [ ] **Leader failover is tested end-to-end**: write → crash leader → verify
      new leader elected → write to new leader → verify both old and new data.
- [ ] **Failover timing is measured** if the spec has an SLA (e.g. "new leader
      within 2 seconds").
- [ ] **Split-brain is tested**: partition the cluster into two groups, write
      to both, heal, verify only one set of writes wins (or that the system
      correctly rejects writes from the minority partition).
- [ ] **Raft log replay is tested**: crash a node, let the cluster make
      progress, restart the crashed node, verify it catches up via log replay.

---

## 6. Erasure Coding Validation

- [ ] **EC volume creation is verified, not silently swallowed.** If the
      `create --erasure-coding` command fails and the test continues with a
      replicated volume, EC is untested. Never use `|| true` on EC volume
      creation.
- [ ] **EC reconstruction is tested**: for RS(k, m), crash exactly m nodes,
      verify data is still readable via reconstruction from k shards.
- [ ] **EC beyond tolerance is tested**: crash m+1 nodes, verify the system
      returns an error (not corrupt data, not a hang). The test must ASSERT
      the error, not discard it.
- [ ] **EC chunk placement is verified**: confirm that chunks actually landed
      on different nodes (not all on the same node). Query the placement map
      or check per-node zvol contents.
- [ ] **EC + concurrent writes**: write workload during node crashes within EC
      tolerance. Verify all acknowledged writes survived with correct data.
- [ ] **EC parity verification**: read back individual shards, verify parity
      checksums match what the RS codec produces from the data shards.

---

## 7. Durability & Persistence

- [ ] **fsync durability test actually crashes after fsync.** The pattern must
      be: write → fsync → kill process (or crash VM) → restart → verify. A test
      that writes, fsyncs, and immediately reads back on the same mount proves
      nothing — page cache serves the read.
- [ ] **Full cluster crash + restart**: crash ALL storage nodes while the
      volume is mounted, restart all, remount WITHOUT reformatting, verify
      previously written data.
- [ ] **Partial write during crash**: write large data, crash mid-write
      (before ack), restart, verify filesystem is consistent (fsck) and
      previously-acked data is intact.
- [ ] **Power-loss vs. process-kill**: process SIGKILL allows the OS to flush
      dirty pages. True power-loss testing requires VM-level crash
      (`qemu-system-x86_64 -monitor quit` or `virsh destroy`). At minimum,
      document which durability guarantee each test validates.

---

## 8. Concurrent I/O Under Fault

The most important class of tests for a distributed storage system. The
combination of concurrent I/O + fault injection catches bugs that neither
finds alone.

- [ ] **Stress + node crash**: run concurrent I/O workload, crash a storage
      node mid-workload, verify all acked writes survived with correct data
      (not just file count).
- [ ] **Stress + network partition**: run workload, partition, verify no
      corruption. Check that writes during partition either succeed (majority
      available) or return errors (minority partition) — never silently corrupt.
- [ ] **Stress + slow disk**: `dm-delay` on one node's backing store, run
      workload, verify latency increases but data remains correct.
- [ ] **Stress + clock skew**: significant clock skew on one node during I/O,
      verify Raft elections and consensus still function.
- [ ] **Multi-client concurrent writes**: two clients writing to overlapping
      regions, verify linearizability or documented consistency model.
- [ ] **Background stress exit code is checked.** If stress runs in the
      background (`nohup`/`&`), the test MUST collect and check its exit code
      after completion.

---

## 9. Synchronization & Timing

- [ ] **No `sleep(N)` as the sole synchronization mechanism.** Every wait
      should be a polling loop with a timeout:
  ```rust
  poll_for(Duration::from_secs(30), Duration::from_millis(500), || {
      ssh_exec(node, "pgrep -x blockyard").is_ok()
  });
  ```
- [ ] **Startup readiness**: after starting a node, poll for process existence
      + port listening + health endpoint, not a fixed sleep.
- [ ] **Post-crash recovery**: after restarting a crashed node, poll for Raft
      membership convergence, not a fixed sleep. Check via cluster status
      endpoint or gossip membership count.
- [ ] **Post-partition heal**: after healing a network partition, poll for
      connection re-establishment and Raft log catch-up, not a fixed sleep.
- [ ] **Test timeout**: every integration test has an overall timeout
      (`#[tokio::test(flavor = "multi_thread", worker_threads = 2)]` +
      `tokio::time::timeout`) so a hung test fails instead of blocking CI
      forever.

---

## 10. Test Naming & Coverage Honesty

- [ ] **Test names match what they test.** A test named `rebalance_*` that
      only crashes a node and checks file integrity is misleading — it's a
      crash tolerance test. Rename or add actual rebalancing.
- [ ] **Rebalancing is tested**: add a node → verify data migrates; remove a
      node → verify data migrates; rebalance during I/O → verify no
      corruption.
- [ ] **Maintenance mode is tested end-to-end**: enter maintenance → verify
      node is not probed/suspected → perform maintenance → exit → verify
      catch-up.
- [ ] **Multi-zpool is tested**: node with multiple pools → verify volumes
      placed across pools; pool failure → verify volumes on healthy pool
      unaffected.

---

## 11. Workload Generator & Consistency Checking

For Jepsen-style validation of distributed correctness:

- [ ] **Wire-protocol-level workload generator exists** — tests the protocol
      layer directly, not through a filesystem. Catches bugs that the
      filesystem layer masks (reordering, coalescing, caching).
- [ ] **Operation log records every write with ack/nack status.** Acked writes
      MUST be durable. Unacked writes MAY be durable.
- [ ] **Consistency checker validates**: every acked write is readable after
      recovery; reads return the latest acked write (linearizability) or
      a documented weaker model.
- [ ] **Checker runs AFTER fault injection and recovery**, not during (when
      transient failures are expected).
- [ ] **`check_all()` includes `check_io_happened()`** — a workload that
      generated zero operations should fail, not pass vacuously.

---

## Summary Scoring

For each test, score on three axes:

| Axis | Score 0 | Score 1 | Score 2 |
|------|---------|---------|---------|
| **Realism** | Mocked/faked infrastructure | Real infra, but page cache or timing issues | Real infra, cache invalidated, real faults |
| **Assertions** | Vacuous or absent | Checks existence/count | Checks data integrity via cryptographic hash |
| **Fault coverage** | Happy path only | Single fault (crash OR partition) | Compound faults (crash + I/O, partition + I/O) |

A test scoring 0 on any axis is providing false confidence and should be
fixed or deleted. The most valuable tests score 2 on all three axes.
