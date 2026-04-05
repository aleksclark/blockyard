# Blockyard

Distributed block-level storage system. CA-prioritizing, Rust, single binary per node, ZFS-backed, UBLK-mounted.

## Design Principles

| Principle | Detail |
|-----------|--------|
| **CA over CP** | Partitions treated as node failures; availability preserved within the majority |
| **Single binary** | One `blockyard` process per node — consensus, storage, replication, client serving |
| **ZFS backend** | Checksums, compression, snapshots, and RAID delegated to ZFS zvols |
| **UBLK mounting** | Userspace block devices via io_uring (Linux 6.0+), no kernel modules |
| **Per-volume config** | Replication factor, write consistency, read policy, affinity — all per-volume |

## Architecture

```
┌───────────────────────────── Blockyard Cluster ─────────────────────────────┐
│                                                                             │
│   Node A              Node B              Node C              Node D        │
│   ┌──────────┐       ┌──────────┐       ┌──────────┐       ┌──────────┐    │
│   │ blockyard│       │ blockyard│       │ blockyard│       │ blockyard│    │
│   │ process  │◄─────►│ process  │◄─────►│ process  │◄─────►│ process  │    │
│   └────┬─────┘       └────┬─────┘       └────┬─────┘       └────┬─────┘    │
│        │                  │                  │                  │           │
│   ┌────┴─────┐       ┌────┴─────┐       ┌────┴─────┐       ┌────┴─────┐    │
│   │ ZFS Pool │       │ ZFS Pool │       │ ZFS Pool │       │ ZFS Pool │    │
│   └──────────┘       └──────────┘       └──────────┘       └──────────┘    │
│                                                                             │
│           gossip discovery  ·  Multi-Raft consensus  ·  block replication   │
└─────────────────────────────────────────────────────────────────────────────┘
        │
        ▼
   Linux Client
   blockyard mount vol-1 → /dev/ublkb0 → ext4/xfs → /mnt/data
```

## Workspace

```
crates/
├── blockyard/            # Main binary — node process with CLI
├── blockyard-cli/        # Lightweight remote management CLI (byard)
├── blockyard-common/     # Shared types, config, errors
├── blockyard-gossip/     # SWIM-based cluster membership
├── blockyard-protocol/   # Client↔cluster wire protocol
├── blockyard-raft/       # Multi-Raft consensus engine
├── blockyard-storage/    # ZFS zvol management + placement engine
└── blockyard-ublk/       # UBLK/NBD volume exposure
```

## Quick Start

```bash
# Build
cargo build --release

# Run a node (requires ZFS pool "blockyard" on the host)
blockyard start --config config/blockyard.example.toml

# Volume operations
blockyard volume create --name web-db --size 100G --replicas 3
blockyard volume list
blockyard volume status web-db

# Mount a volume
blockyard mount web-db
# → /dev/ublkb0
```

## Configuration

See [`config/blockyard.example.toml`](config/blockyard.example.toml) for the full reference.

Minimal:
```toml
[node]
listen = "0.0.0.0:7400"

[cluster]
seeds = ["10.0.1.1:7400"]

[storage]
zfs_pool = "blockyard"
```

## Status

**Phase 1 — Core (MVP)**: in progress. See [ROADMAP.md](ROADMAP.md) for detailed status.

## Requirements

- Rust 1.85+
- Linux with ZFS (zpool pre-created)
- Linux 6.0+ for UBLK (NBD fallback for older kernels)
