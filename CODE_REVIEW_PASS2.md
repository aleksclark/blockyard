# Code Review Pass 2 — Phases 5-7 + Integration Tests

Reviewed 2026-04-07 by 3 parallel reviewers.

## CRITICAL

### C1. TokenBucket race condition (rate_limit.rs:37-68, 100-117)
Mixed Mutex (refill) + AtomicU64 (acquire) creates split-brain. refill() does load->compute->store while holding Mutex, but try_acquire() CAS-decrements without the Mutex. Concurrent refill+acquire can silently revert consumed tokens.
Fix: Use Mutex for all operations, or use a proper CAS loop.

### C2. Rebalancing integration tests are still placeholders
All 4 tests in rebalancing.rs call simulate_rebalance()/simulate_drain() — local functions returning synthetic counts. No real crate logic exercised.

### C3. No integration test for LeaseManager + WritePipeline flow
Lease manager has unit tests but no integration test verifies that WritePipeline checks lease validity before writing or that lease loss stops writes.

## IMPORTANT

### Storage

I1. Drain worker claims success before repairs complete (drain.rs:161) — progress.relocated incremented on enqueue, not completion. Disk transitions to Removed prematurely.

I2. Operation log eviction is non-deterministic (service.rs:353-364) — HashMap iteration order is arbitrary, may evict recent ops while keeping old ones. Use ordered structure.

I3. RepairWorker::run busy-loops when queue has items (repair.rs:405-427) — never checks cancellation inside processing branch. Cannot be stopped gracefully.

I4. No duplicate detection in repair queue (repair.rs:156-165) — same extent_id can be enqueued multiple times from scrub and drain.

I5. Rebalance always targets first under-utilized disk (rebalance.rs:200) — no rotation, risks making the target over-utilized.

I6. ScrubWorker::run never scrubs immediately on startup (scrub.rs:253-266) — first scrub after interval_secs (24h default).

I7. Scrub error classification via string matching (scrub.rs:197) — fragile, should use typed errors.

I8. Extent commit: .meta written before data rename (extent.rs:281-306) — crash between meta write and rename leaks orphan .meta file.

### Raft / Common

I9. MetadataService missing Debug derive (service.rs:31) — violates AGENTS.md.

I10. validate_lease returns String not typed error (state_machine.rs:447) — inconsistent with thiserror convention.

I11. Stale read risk on local lease validation (service.rs:210-226) — get_lease/validate_lease read local state, may lag on followers. Must document leader-only requirement.

I12. No lease-related ErrorCode in protocol (messages.rs:94-105) — no LeaseDenied/LeaseExpired/LeaseFenced variants.

### Client / UBLK

I13. resolve_batch() bails on first error (ambiguous_write.rs:111-138) — leaves remaining ops unresolved.

I14. LeaseManager mark_lost() doesn't clear lease_version (lease_manager.rs:197-200) — callers checking version independently get stale value.

I15. test_partition_convergence doesn't actually inject a partition (data_integrity.rs) — just tests normal replication.

I16. Availability partition test "heals" by creating brand-new empty nodes (availability.rs:363-382) — doesn't test actual partition healing.

I17. test_mount_write_crash_remount_verify "crash" is trivial (ublk_client.rs) — in-memory Arc data survives by design, not testing real crash semantics.

I18. MetadataClient trait bloat from lease methods — 8+ mock implementations needed across test files. Consider splitting lease methods.

I19. CoalescingBuffer merge doesn't handle overlapping byte ranges (ec_write_pipeline.rs:114-123) — concatenation instead of proper overlay.

### Background Workers

I20. RepairConfig.max_concurrent not enforced (repair.rs:20) — declared but never checked.

I21. Scheduler doesn't actually coordinate priority between workers (scheduler.rs) — TaskPriority enum defined but not connected to token allocation. All workers share the same TokenBucket equally.

## MINOR

M1. extent_meta_path uses unwrap without SAFETY comment (extent.rs:160)
M2. XFS error detection via string matching (service.rs:391-407)
M3. ec_read_pipeline source_node uses NodeId::generate() fallback when empty
M4. session.rs OperationId counter is decorative — generates random UUID, comment says "monotonically increasing"
M5. Global EXTENT_VERSION_COUNTER shared across all pipelines
M6. DiskInventory::discover_disks fails fast on first error
M7. No Serialize/Deserialize on background result types
M8. MetadataResponse missing PartialEq derive
M9. LeaseRequest missing PartialEq derive
M10. WriteExtentRequest.lease_version is Option with no docs on when None is acceptable

## What Was Fixed Since Last Review

- snapshot_idx is now Arc<AtomicU64> (C9 from last review) ✓
- blake3 checksum with shared function (C4 from last review) ✓
- Parallel replica writes with JoinSet (C6 from last review) ✓
- EC read metadata refresh (C5 from last review) ✓
- Operation log eviction added (I1 from last review) — but non-deterministic order (new I2)
- Watermark changed from EpochId (I4 from last review) ✓
- StaleEpochHandler dedup added (I8 from last review) ✓

## Priority Actions

1. **Fix TokenBucket** (C1) — use Mutex for all operations
2. **Fix drain completion tracking** (I1) — wait for repairs, not just enqueues
3. **Fix repair queue busy-loop** (I3) — check cancellation in processing branch
4. **Add ordered eviction** (I2) — evict oldest operations, not random
5. **Add lease ErrorCode variants** (I12) — LeaseDenied, LeaseExpired, LeaseFenced
6. **Add Debug to MetadataService** (I9)
7. **Fix CoalescingBuffer merge** (I19) — proper byte overlay, not concatenation
