# Design decisions

## ADR-000: Name
Drumlin: a hill built from layers of sediment, like the levels of an LSM tree. GitHub search 2026-11-10 found 84 repos with the name, none in storage.

## ADR-001: Go, one package, standard library only
Matches Pitbook and cache-cluster, and keeps the project dependency-free. The store is one package (`db`) so internals can be tested directly (crash simulation, diagnostics) without exporting them.

## ADR-002: Sequence-numbered versions instead of locks for reads
Snapshots and compaction correctness both fall out of "every entry has a sequence number". The cost is that compaction must reason about live snapshots (see `keep`).

## ADR-003: The log is handed to the OS on every write; fsync is optional
Handing the record to the OS before acknowledging survives a process crash. `Options.Sync` adds fsync for power-loss safety at a large throughput cost. The crash harness kills processes, so it verifies the first guarantee, not the second; this is stated everywhere results are.

## ADR-004: JSON manifest rewritten whole
Simple and atomic via rename. It is O(files) per change, fine at this scale, and the wrong choice for millions of files. A real system uses an append-only manifest log.

## ADR-005: Per-block CRC32C and a checksummed footer
Corruption must be detected, not served. Tested by flipping bytes in blocks and in the footer.

## ADR-006: Cut compaction outputs only between user keys
Splitting versions of one key across files would break "one file per level" point reads and snapshot reads. The cost is slightly uneven file sizes.

## ADR-007: Single background worker, one frozen memtable
Flush and compaction share one goroutine, so a long compaction can delay a flush and stall writers. Kept for simplicity; the benchmark shows the stall in the tail latency instead of hiding it.

## ADR-008: Raft is phase B and is not claimed
Replication is a separate project-sized piece (election, log replication, snapshots, a linearizability checker). Phase A stands alone and is the only thing claimed.
