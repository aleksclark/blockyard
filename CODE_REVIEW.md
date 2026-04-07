# Code Review — Phases 0-4D

Reviewed 2026-04-07 by 3 parallel reviewers. 15,416 lines across 9 crates, 585 tests.

## CRITICAL (must fix)

### C1. sync_parent_dir silently swallows errors — breaks crash consistency
**storage/extent.rs:522-529** — `sync_all()` failure is ignored with `let _ =`. This undermines invariant 9 (local durability acks are crash-consistent). Must propagate errors.

### C2. Double DiskId::generate() in write_error — dedup record mismatch
**storage/service.rs:406,416** — Generates two different random DiskIds: one for the OperationRecord, another for the WriteExtentResponse. Client dedup won't match. Generate once and reuse.

### C3. Epoch validation arithmetic overflow
**storage/service.rs:318** — `request_epoch.as_u64() + 1` wraps at u64::MAX. Use `saturating_add` or restructure the comparison.

### C4. Checksum is FNV-1a — not a strong hash
**write_pipeline.rs:321, ec_write_pipeline.rs:530, ec_read_pipeline.rs:277** — Three copies of FNV-1a. Spec requires "strong checksum" (§5.3). Need blake3 or xxhash. Also consolidate to a single shared function.

### C5. EC read pipeline doesn't refresh stale metadata
**ec_read_pipeline.rs:98-104** — Returns StaleMapping without attempting refresh. Spec §4.4: "MUST refresh metadata before serving the read." Replicated read does this correctly; EC does not.

### C6. Sequential replica/fragment writes — latency = sum not max
**write_pipeline.rs:279-307, ec_write_pipeline.rs:389-427** — Writes to replicas are sequential `for` loop with `.await`. Must use parallel fanout (JoinSet/FuturesUnordered).

### C7. EC durability requires ALL K+M acks — too strict
**ec_write_pipeline.rs:323-328** — Spec says "sufficient to satisfy the protection policy." EC can tolerate up to M failures. Requiring all acks makes EC unnecessarily fragile.

### C8. Coalescing buffer silently drops data on same-stripe overwrite
**ec_write_pipeline.rs:102-121** — BTreeMap::insert replaces previous entry. Two writes to the same stripe: first write's data is LOST. Must merge within the stripe.

### C9. Raft snapshot_idx not shared across clones
**raft/store.rs:160-163** — `snapshot_idx: u64` is copied on clone. Concurrent snapshot builds get duplicate IDs. Should be `Arc<AtomicU64>`.

## IMPORTANT (should fix)

### I1. Operation log grows unbounded — no eviction
**storage/service.rs:34** — `operation_log: HashMap<OperationId, OperationRecord>` never pruned. Will OOM eventually. Need TTL or max-size.

### I2. Raft reads are local-only — can return stale data
**raft/service.rs:143-149** — lookup methods read from local state machine without linearizable read. On followers, returns stale data. Must document or fix.

### I3. Raft storage is in-memory only — P3.7 (crash recovery) incomplete
**raft/store.rs** — All state lost on restart. ROADMAP marks P3.7 done but crash recovery requires persistent log.

### I4. WriteWatermark uses EpochId but should be commit sequence
**watermark.rs** — Epoch doesn't advance on every commit (only topology changes). Breaks read-your-own-writes if multiple commits share an epoch.

### I5. extent_version = epoch*1000+block_start — collision-prone
**write_pipeline.rs:151, ec_write_pipeline.rs:289** — Two writes to same block in same epoch get same version. Use operation counter or proper generator.

### I6. Cross-crate trait divergence — DataNodeClient vs DataNodeReader
**ublk/traits.rs vs client/traits.rs** — Independent trait hierarchies for same data nodes. Should share traits in common or a new crate.

### I7. Replicated write requires ALL replicas — not policy-flexible
**write_pipeline.rs:186-191** — Spec allows N-1 (majority) acks. Currently all-or-nothing.

### I8. StaleEpochHandler race — concurrent callers all refresh
**stale_epoch.rs:56-93** — Multiple callers can all trigger refresh simultaneously. Need dedup (Once or mutex).

### I9. Sequential EC fragment reads
**ec_read_pipeline.rs:161-217** — Same as C6 but for reads. Must parallelize.

### I10. validate_xfs accepts any directory on Linux
**storage/disk.rs:326-359** — Falls through to Ok(()) for non-XFS. Defeats invariant 8.

### I11. ExtentIndex keyed by ExtentId only — one version per extent
**storage/extent.rs:62** — Can't track multiple versions. Key should be (ExtentId, version).

### I12. No tracing in raft crate
Zero logging in state machine, snapshots, log purge. Will be undebuggable in production.

### I13. ErrorCode enum in protocol is dead code
**protocol/messages.rs:91-101** — Defined but never used. Responses use bool+String.

### I14. No multi-node Raft integration tests
All tests are single-component. No leader election, replication, or failover tests.

## MINOR

- M1. All modules `pub` instead of `pub(crate)` with re-exports (common, storage, protocol, raft)
- M2. checksum fields are String in some places, Vec<u8> in others
- M3. BTreeMap<String, ...> where typed ID keys would be better (raft state machine, metadata cache)
- M4. MetadataService missing `#[derive(Debug)]`
- M5. OperationId counter in session.rs is decorative (generates random UUID anyway)
- M6. Unused deps in raft Cargo.toml (bytes, futures, rand)
- M7. No Serialize/Deserialize on EC pipeline types
- M8. CoalescingBuffer time-based flush needs background task (currently only checked on add_write)
- M9. BadRegionMap doesn't merge overlapping regions
- M10. config validation doesn't check AuthSection
- M11. `extent_meta_path` unwrap needs SAFETY comment

## Recommendations for Phase 5 dispatch

The code review findings fall into two categories:
1. **Fixable now** — bugs and spec violations (C1-C9, I1, I4, I5, I7, I8)
2. **Deferred** — architecture changes that would happen with Phase 5+ anyway (I2, I3, I6, I12, I14)

Suggest: dispatch a "review fixes" agent alongside Phase 5 to address category 1.
