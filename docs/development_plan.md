# Development plan and status

## Phase A: single node (done 2026-11-10)
- [x] Skiplist memtable, WAL with CRC framing and torn-tail truncation
- [x] SSTable format with block checksums, Bloom filter, block index, footer
- [x] Manifest with atomic replace; recovery that replays logs and cleans leftovers
- [x] Leveled compaction with snapshot-aware version retention
- [x] Snapshots, ordered scans, write-amplification counters
- [x] Differential, torn-log, corruption, snapshot, concurrency tests
- [x] Crash harness, 1,000 kills, 0 violations
- [x] Mutation check, 7 of 7
- [x] Benchmarks
- [ ] CI run on GitHub (workflow written, not yet run)

## Next (not started)
- [ ] Group commit (the fsync cost is the first thing to fix)
- [ ] Block cache, compression, append-only manifest log
- [ ] **Phase B: Raft** (leader election, log replication, snapshots) and a Jepsen-style fault harness with a linearizability checker. This is the larger half of the original scope and has not been started.
