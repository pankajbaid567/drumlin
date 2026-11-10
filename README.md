# Drumlin

**LSM-tree key-value store, then Raft replication.** Working name; checked on GitHub 2026-11-10 (84 repos with this name, none in storage). Status: **planned**.

A drumlin is a hill built up from layers of sediment, which is what an LSM tree's levels are.

## Why (JD coverage)
Rubrik, Nutanix, Cloudflare, Confluent, Atlassian infrastructure teams, storage and database teams. The research found that only 14% of selected candidates show anything distributed-systems shaped, and replication and consensus is the biggest gap left in this portfolio (Pitbook is single node, cache-cluster has no replication).

## Scope (v1, to be confirmed in the plan)
Phase A: single node. Memtable, write-ahead log, SSTables with block index and bloom filters, leveled compaction, snapshot reads, crash recovery, db_bench-style benchmarks and write amplification numbers.
Phase B: replication. Raft (leader election, log replication, snapshots, membership change kept minimal) on top of the same log, linearizable reads, a Jepsen-style fault harness (partitions, kills, clock skew) that checks linearizability.

## Non-goals
Transactions, SQL, multi-region, production claims.

## Proof targets (set up front, not results)
Crash-recovery harness with 0 lost acknowledged writes over at least 1,000 kills; linearizability checker passing under partitions; throughput and write amplification reported with method.

## Reuses
Pitbook (WAL framing, group commit, crash harness, fuzzing and torn-tail tests, benchmark method) and cache-cluster (Go project layout).

## Risks
Raft is easy to get subtly wrong; the fault harness is the real deliverable. Phase A must stand on its own on the resume if Phase B slips.
