# Blockyard Client and Data Node Specification

## Status of This Memo

This document specifies normative requirements for the Blockyard client and data node components. The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHALL**, **SHALL NOT**, **SHOULD**, **SHOULD NOT**, **RECOMMENDED**, **NOT RECOMMENDED**, **MAY**, and **OPTIONAL** in this document are to be interpreted as described in RFC 2119 and RFC 8174 when, and only when, they appear in all capitals.

---

## 1. Introduction

Blockyard is a distributed block-level storage system in which clients access cluster-backed virtual block devices through `ublk`, and storage nodes provide both data services and colocated metadata services.

This specification defines:

- The normative behavior of the Blockyard client.
- The normative behavior of the Blockyard data node.
- The interaction between client and node during read, write, recovery, and membership change workflows.
- Required behavior under failure conditions, including stale metadata, disk faults, node faults, quorum loss, and partial IO completion.

This document assumes the following architectural properties:

- Clients communicate directly with storage nodes; no gateway tier exists.
- Metadata and placement services are colocated with data nodes.
- Each physical data disk on a node contains exactly one XFS filesystem dedicated to Blockyard storage.
- User data is stored as extent files on those per-disk XFS filesystems.
- Cluster durability is provided by configurable replication or erasure coding.
- The system prioritizes consistency over partition availability.
- The system MUST provide read-your-own-writes semantics for a single client session.

---

## 2. Terminology

### 2.1 Client
A userspace process servicing a `ublk` device and acting as the protocol endpoint on behalf of a host.

### 2.2 Data Node
A cluster node that stores user data extents and participates in the metadata quorum.

### 2.3 Metadata Service
The strongly consistent replicated state machine that stores cluster membership, placement policy, volume metadata, extent mappings, and protection policy.

### 2.4 Placement Epoch
A monotonically increasing metadata version that identifies a specific cluster map and placement state.

### 2.5 Volume
A logical block device exported to a client.

### 2.6 Extent
A contiguous logical block range represented by one or more extent files or fragments on data nodes.

### 2.7 Extent File
A file stored on a node-local XFS filesystem representing a single local replica extent or erasure-coded fragment extent.

### 2.8 Session Write Watermark
The highest committed metadata version or commit sequence observed by a client session.

### 2.9 Disk State
A node-local state for a physical disk. Valid states are `healthy`, `suspect`, `degraded`, `draining`, `failed`, and `removed`.

### 2.10 Protection Policy
The configured durability policy for an extent or volume, such as replication factor N or erasure coding K+M.

### 2.11 Commit
The point at which metadata has durably recorded an extent mapping as the current visible version.

---

## 3. Protocol and System Model

### 3.1 Cluster Model
Each data node SHALL host:

1. A data service for storing and serving extent files.
2. A metadata service participant.
3. A local disk manager.
4. Background repair, scrub, and rebalance workers.

Each client SHALL:

1. Serve one or more `ublk` devices.
2. Cache metadata and placement information.
3. Compute placement using committed metadata state.
4. Communicate directly with data nodes for data IO.
5. Communicate with the metadata service for commit, refresh, and fencing operations.

### 3.2 Consistency Model
The system SHALL provide:

- Strong consistency for committed metadata updates.
- Read-your-own-writes consistency for a single client session.
- No guarantee of write availability in minority partitions.

A client MUST NOT treat locally buffered or partially replicated data as committed until the corresponding metadata update is durably committed by the metadata quorum.

### 3.3 Local Storage Model
Each physical data disk on a data node SHALL host exactly one XFS filesystem dedicated to Blockyard extent files.

A data node MUST NOT stripe a single local extent file across multiple local disks.

A data node MAY maintain multiple node-local storage classes, but each class MUST be composed of individually managed disks rather than a local RAID abstraction for user data durability.

---

## 4. Client Specification

## 4.1 Client Responsibilities
The client SHALL:

- Export a block device through `ublk`.
- Translate block read and write requests into Blockyard protocol operations.
- Maintain a cache of volume metadata, placement epoch, and extent mappings.
- Track a session write watermark.
- Retry or refresh metadata when placement is stale.
- Enforce write ordering and completion visibility rules defined in this specification.

The client MUST behave as a correctness participant, not merely a transport endpoint.

## 4.2 Client Identity and Session
A client session MUST possess a stable session identifier for the lifetime of a mounted device instance.

The client MUST associate each write operation with:

- The target volume identifier.
- A logical block range.
- A session identifier.
- An operation identifier unique within the client session.

The client SHOULD preserve operation identifiers across safe retries.

## 4.3 Metadata Cache
The client MAY cache the following:

- Placement epoch.
- Cluster membership and placement map.
- Volume protection policy.
- Extent mappings.
- Fencing or ownership metadata, if applicable.

The client MUST treat cached metadata as advisory unless it has verified that the metadata is sufficiently fresh for the requested operation.

For any read issued after a committed write in the same session, the client MUST ensure that the metadata view used for the read is at least as new as the client session write watermark.

## 4.4 Session Write Watermark
The client MUST maintain a monotonically non-decreasing session write watermark.

When a write commit succeeds, the client MUST advance the watermark to the returned commit version.

The client MUST attach the current watermark, or an equivalent minimum required visible version, to subsequent reads for that volume in the same session.

If the client cannot verify that a read will observe data at or above the watermark, it MUST refresh metadata before serving the read.

## 4.5 Client Write Path
### 4.5.1 General
For each write request, the client SHALL:

1. Validate that it holds any required ownership, fencing, or lease authority.
2. Resolve the logical block range against current metadata.
3. Determine the applicable protection policy.
4. Compute target placement from the current placement epoch.
5. Create a new extent version.
6. Transmit data or fragments to target nodes.
7. Await sufficient durability acknowledgments.
8. Submit the metadata commit.
9. Acknowledge the write to the kernel only after commit success.

The client MUST NOT acknowledge write completion to `ublk` before the metadata commit has succeeded.

### 4.5.2 Replicated Write
For replicated data, the client MUST send the new extent payload to the target replica set selected by placement.

The client MUST require acknowledgments from the durability threshold defined by policy before attempting commit. If policy requires all replicas before commit, the client MUST wait for all such acknowledgments.

The client MUST include in the metadata commit request:

- Volume identifier.
- Logical block range.
- New extent version identifier.
- Placement epoch used.
- Replica locations.
- Checksums.
- Previous mapping version, if compare-and-swap semantics are required.

### 4.5.3 Erasure-Coded Write
For erasure-coded data, the client MUST:

- Determine the stripe geometry for the target range.
- Encode user data into K data fragments and M parity fragments.
- Send each fragment to the designated node placement.
- Require acknowledgments sufficient to satisfy the protection policy.
- Commit the new fragment mapping only after the policy’s durability condition has been met.

If partial-stripe overwrite is supported, the client MUST perform read-modify-write or equivalent reconstruction logic sufficient to preserve correctness.

The client SHOULD coalesce adjacent writes to reduce partial-stripe amplification.

### 4.5.4 Idempotent Retry
The client SHOULD make write transmissions idempotent.

If the client retries a write due to timeout or ambiguous completion, it MUST use the same operation identifier or an equivalent deduplication token.

A data node receiving a duplicate operation identifier for the same extent version MUST respond in a way that does not create duplicate durable state.

## 4.6 Client Read Path
For each read request, the client SHALL:

1. Determine the minimum visible version required by the session write watermark.
2. Resolve the current extent mapping for the target logical block range.
3. Verify that the mapping version is at least the minimum required version.
4. Select an appropriate source replica or fragment set.
5. Issue data reads.
6. Verify checksums where applicable.
7. Return data to the kernel.

If a selected source fails, the client MAY retry another replica or reconstruct from erasure-coded fragments.

The client MUST NOT return data from an extent version older than the minimum required version for the session.

## 4.7 Stale Epoch Handling
If any data node or metadata service indicates that the client is using a stale placement epoch, the client MUST:

1. Stop issuing new data writes using that stale epoch.
2. Refresh the cluster map and placement information.
3. Re-resolve the operation under the new epoch.
4. Retry only if doing so cannot create conflicting committed mappings.

The client MUST treat a stale-epoch rejection as retriable, not as a terminal IO failure, unless refresh or re-resolution fails.

## 4.8 Ownership and Fencing
If Blockyard supports exclusive write ownership for a volume, the client MUST obtain and periodically renew the required fencing token or lease.

A client whose lease has expired, been revoked, or cannot be confirmed MUST NOT issue new writes.

A client MAY continue reads only if permitted by volume policy and if read consistency rules can still be met.

## 4.9 Client Behavior Under Failure Conditions

### 4.9.1 Metadata Quorum Unavailable
If the metadata quorum is unavailable, the client MUST NOT acknowledge new writes as successful.

The client MAY continue reads only if:

- The read can be satisfied from a mapping already known to be committed.
- The required session watermark can be met.
- Volume policy permits such reads.

If these conditions cannot be met, the client MUST fail the read.

### 4.9.2 Ambiguous Write Completion
If the client has transmitted data to one or more nodes but cannot determine whether metadata commit succeeded, the client MUST treat the write outcome as ambiguous.

For an ambiguous write, the client MUST:

- Query metadata for the operation identifier or extent version if supported, or
- Re-read the current extent mapping before retrying.

The client MUST NOT blindly issue a new logically conflicting write intended to replace the ambiguous write without first determining the committed state.

### 4.9.3 Partial Replica or Fragment Acknowledgment
If some but not enough targets acknowledge the write for commit eligibility, the client MUST NOT commit the mapping.

The client SHOULD instruct successful targets to discard or garbage-collect uncommitted data after a safe interval, or MUST rely on background orphan cleanup.

### 4.9.4 Read Source Failure
If a read source fails or returns corrupt data, the client MUST:

- Retry another healthy source if available, or
- Trigger reconstruction if policy permits, or
- Fail the read if no valid recovery path exists.

The client SHOULD report the failure to the metadata service or health subsystem for scheduling and repair decisions.

### 4.9.5 Session Restart
After client restart, read-your-own-writes guarantees apply only to writes whose commit versions have been durably recovered or re-established for the new session.

A client implementation MAY persist watermark state across restart, but if it does so it MUST ensure the persisted state is itself crash-consistent.

---

## 5. Data Node Specification

## 5.1 Data Node Responsibilities
A data node SHALL:

- Store local replicas or fragments as extent files on per-disk XFS filesystems.
- Serve read and write requests for those local extents.
- Participate in metadata quorum operations.
- Maintain disk inventory and health state.
- Run scrub, recovery, and rebalance workflows.
- Refuse operations inconsistent with current metadata epoch or local health policy.

## 5.2 Local Disk Model
Each physical data disk MUST:

- Be individually discoverable and identifiable.
- Contain exactly one XFS filesystem dedicated to Blockyard extent files.
- Possess a persistent Blockyard disk identifier.
- Expose a node-local health state.

A data node MUST maintain a mapping from local extent identifier to:

- Disk identifier.
- XFS path or inode reference.
- Extent version.
- Checksum metadata.
- Allocation class or storage class.

A data node MUST NOT store user extents on a disk in state `failed` or `removed`.

A data node MUST NOT allocate new user extents on a disk in state `degraded` or `draining`.

## 5.3 Extent File Requirements
Each extent file MUST be immutable after successful local durability acknowledgment for a committed extent version.

A node MAY create temporary files during write staging, but it MUST NOT expose a staged extent as committed local state until local durability conditions are satisfied.

Each extent file MUST have associated integrity metadata sufficient to verify correctness on read and scrub. This integrity metadata MUST include at least one strong checksum of the stored payload.

## 5.4 Local Durability Acknowledgment
A data node MUST acknowledge a local write as durable only after:

- The extent file contents are durably persisted according to configured durability rules.
- The extent file metadata required for later retrieval is durably persisted.
- Required integrity metadata has been recorded.

The exact mechanism MAY use fsync, fdatasync, O_DSYNC semantics, or equivalent operating system guarantees, but the implementation MUST ensure crash-consistent recovery of acknowledged local state.

## 5.5 Write Reception
Upon receiving a client write for a replica or fragment, the node MUST:

1. Validate the placement epoch or reject if stale.
2. Validate disk and pool eligibility.
3. Stage the extent file on an eligible local disk.
4. Persist payload and integrity metadata.
5. Record operation identifier and local extent metadata sufficient for deduplication or crash recovery.
6. Return success only after local durability is satisfied.

A node SHOULD preserve enough metadata to identify duplicate retransmission of the same operation.

## 5.6 Read Service
Upon receiving a read request, the node MUST:

- Locate the referenced local extent.
- Verify that the local extent is in readable state.
- Read the requested byte range.
- Validate data integrity according to local checksum policy.
- Return data only if integrity checks succeed.

If a checksum mismatch or read error occurs, the node MUST fail the read and SHOULD mark the underlying disk or region as suspect.

The node MUST NOT knowingly return corrupted data.

## 5.7 Metadata Participation
A data node participating in the metadata quorum MUST:

- Apply committed metadata entries in order.
- Expose the current committed placement epoch.
- Reject or redirect operations that depend on stale epoch state.
- Ensure that applied metadata state is crash recoverable.

A node MUST distinguish clearly between:

- Data durability acknowledgment for local extent storage.
- Metadata commit acknowledgment as a quorum participant.

A node MUST NOT imply that local storage success alone means a client write is globally committed.

## 5.8 Local Disk Health Management
The node SHALL maintain per-disk telemetry including, at minimum where supported:

- Read errors.
- Write errors.
- Checksum mismatches attributable to local reads.
- Device-reported media errors.
- Timeouts and transport resets.
- Temperature and wear indicators for SSD or NVMe.
- Latency outlier statistics.

The node MUST derive a disk state from observed telemetry and policy.

### 5.8.1 Disk State Effects
- `healthy`: new allocations and reads are permitted.
- `suspect`: reads are permitted; new allocations SHOULD be reduced or deprioritized.
- `degraded`: reads MAY continue; new allocations MUST NOT occur; evacuation SHOULD begin.
- `draining`: reads MAY continue; new allocations MUST NOT occur; data movement MUST proceed.
- `failed`: neither reads nor allocations SHALL be attempted except for explicit diagnostic action.
- `removed`: the disk SHALL NOT be referenced for user data.

## 5.9 Bad Region Handling
If repeated read or write failures occur at a localized disk region, the node SHOULD maintain a bad-region map.

The node MUST NOT place new extents in a quarantined bad region.

If existing extent files are affected by a quarantined region, the node SHOULD report them for repair or evacuation.

## 5.10 Disk Add and Discovery
A node MUST support online disk discovery.

When a new disk is added, the node SHALL:

1. Detect the device.
2. Validate that it is not already bound to another active Blockyard node unless explicitly re-provisioned.
3. Initialize or validate the dedicated XFS filesystem.
4. Assign or recover the Blockyard disk identifier.
5. Add the disk in a non-serving qualification state if policy requires burn-in.
6. Transition the disk to `healthy` only after qualification succeeds.
7. Advertise the new writable capacity to the metadata service.

The node MUST NOT advertise a newly discovered disk as available for user allocations before local initialization has succeeded.

## 5.11 Disk Drain and Removal
The node MUST support operator-initiated disk draining.

When a disk enters `draining` state, the node MUST:

- Stop all new allocations on that disk.
- Enumerate resident live extents.
- Report those extents for repair, relocation, or rebuild according to cluster policy.
- Continue serving reads where possible until the disk becomes unreadable or empty.

A disk MAY be removed from service only after either:

- No live extents remain, or
- Remaining extents have been declared lost and scheduled for cluster recovery.

## 5.12 Background Scrubbing
A node MUST implement local scrubbing.

Local scrubbing SHALL verify:

- Readability of extent files.
- Integrity checksums.
- Recoverability of local metadata.

Scrub-detected corruption or unreadability MUST be reported to the health subsystem and SHOULD trigger repair workflows.

## 5.13 Rebalance and Repair Participation
A node SHALL support background data movement for:

- Re-replication.
- Erasure-code rebuild.
- Disk drain.
- Capacity rebalance.

A node MUST rate-limit or schedule such work so that metadata responsiveness and foreground IO are not starved.

---

## 6. Failure Condition Requirements

## 6.1 Client Crash During Write
If a client crashes after transmitting data but before write acknowledgment to the kernel, the write outcome MAY be either committed or uncommitted.

After restart, the system MUST resolve correctness by committed metadata state, not by presence of orphaned local extent files alone.

Uncommitted local extent files MUST NOT become visible to reads.

## 6.2 Data Node Crash After Local Acknowledgment
If a node crashes after returning local durability success but before the client receives the response, the client SHALL treat the outcome as ambiguous.

On recovery, the node MUST either:

- Preserve enough local state to honor duplicate suppression and resume correct behavior, or
- Allow the client to determine committed state through metadata interrogation without risking duplicate committed mappings.

## 6.3 Data Node Crash Before Local Durability
If a node crashes before local durability is complete, it MUST NOT later claim that the write was successfully persisted.

Partially staged files remaining after crash recovery MUST be treated as uncommitted and MUST NOT be exposed as valid local extents.

## 6.4 Metadata Quorum Partition
In a network partition where a node or set of nodes loses metadata quorum, those nodes MUST NOT accept new metadata commits.

Clients attached only to a minority partition MUST NOT be able to successfully commit new writes.

Reads in a minority partition MAY continue only if they are allowed by policy and can be satisfied without violating session consistency requirements.

## 6.5 Stale Placement During Topology Change
If topology changes and the placement epoch advances while a client is issuing IO, nodes MUST reject stale-epoch writes.

Clients MUST refresh metadata before retrying.

A node MAY serve stale-epoch reads only if the referenced extent remains valid and readable, but the client remains responsible for ensuring session consistency.

## 6.6 Disk Failure
If a disk fails, the node MUST:

- Transition that disk to `failed`.
- Stop new reads and writes to that disk except optional diagnostics.
- Report the affected extent set for cluster repair.
- Exclude the disk’s capacity from future placement.

The node SHOULD remain otherwise serviceable if enough healthy local resources remain.

## 6.7 Corruption Detection on Read
If corruption is detected during read:

- The node MUST fail the read of that local source.
- The client MUST retry alternate recovery paths if available.
- The system SHOULD record the event against the disk health and repair subsystems.

The corrupted local copy or fragment MUST NOT continue to be treated as healthy without repair or operator override.

## 6.8 XFS-Level Local Filesystem Error
If the dedicated XFS filesystem on a disk becomes unavailable, shut down, or enters an error state that prevents reliable access, the node MUST treat the corresponding disk as at least `degraded`, and as `failed` if the extent files can no longer be safely read.

The node MUST NOT continue allocating extents on such a filesystem.

## 6.9 Orphaned Extent Files
Extent files may become orphaned if local durability succeeded but metadata commit never occurred.

Orphaned extent files MUST NOT be visible through the committed extent map.

The node SHOULD reclaim orphaned extent files after a safe retention interval and after ensuring they are not referenced by any committed metadata state.

## 6.10 Restart and Recovery Ordering
On node startup, local recovery MUST restore enough state to:

- Serve committed extent files.
- Prevent exposure of uncommitted staged files.
- Resume health and scrub tracking.
- Rejoin metadata participation only after required local metadata state is valid.

A node MUST NOT advertise itself as fully writable before local recovery has completed.

---

## 7. Interoperability and Versioning

Clients and data nodes MUST support protocol version negotiation.

A client MUST NOT issue operations requiring semantics unknown to the contacted node version.

A node MUST reject unsupported operation versions explicitly.

Metadata epoch, protocol version, and feature flags SHOULD be included in relevant operation handshakes.

---

## 8. Security and Isolation Considerations

Clients MUST authenticate to the cluster.
Nodes MUST authenticate peer metadata participants and clients.

A node MUST validate that a client is authorized for a target volume before serving IO.

If fencing or exclusive ownership is enabled, a node and metadata service MUST reject writes from clients lacking valid authority.

Checksums and integrity validation protect against corruption but do not replace authentication or authorization.

---

## 9. Operational Observability Requirements

The implementation MUST expose observability sufficient to determine:

- Per-volume IO success and failure rates.
- Client watermark and stale-epoch retry counts.
- Per-node foreground and background IO load.
- Per-disk health state transitions.
- Scrub findings.
- Repair backlog.
- Orphaned extent file counts.
- Metadata quorum health and commit latency.

Disk-level health observability SHOULD include stable disk identifiers, not merely transient device names.

---

## 10. Summary of Normative Invariants

The following invariants are REQUIRED:

1. A client MUST NOT acknowledge a write before metadata commit success.
2. A node MUST NOT return corrupted data knowingly.
3. Uncommitted extent files MUST NOT become visible to reads.
4. Stale-epoch writes MUST be rejected.
5. Read-your-own-writes MUST be enforced through session watermark semantics.
6. New allocations MUST NOT occur on `degraded`, `draining`, `failed`, or `removed` disks.
7. Disk failures and local corruption MUST be surfaced to cluster repair logic.
8. Each physical data disk MUST host exactly one XFS filesystem dedicated to Blockyard extent files.
9. Local extent durability acknowledgments MUST be crash-consistent.
10. Minority partitions MUST NOT be allowed to commit conflicting writes.

---

## 11. Future Work

The following areas are intentionally left for future specification:

- Exact on-wire RPC definitions.
- Metadata log and consensus protocol selection.
- Lease and fencing protocol details.
- Snapshot and clone semantics.
- Multi-client write arbitration for shared volumes.
- Detailed garbage collection protocol for superseded and orphaned extents.
- Exact XFS mount and tuning requirements.
- Detailed erasure-coded partial-stripe update semantics.

