# Results

Measured 2026-11-10 on one laptop (Apple M3, 8 cores, 8 GB, macOS, Go 1.25.4). The machine was not idle. Numbers are for this implementation on this machine, not for any other store.

## Correctness
- `go test -race ./...`: 10 tests pass. The differential test also passes at `DRUMLIN_LONG=1` (300,000 random operations against a map, with reopen and simulated crashes).

### Crash campaign: 1,000 kills, 0 violations
`./bin/crashtest -runs 1000 -seed 20261110` (raw rows: `docs/results/crash-campaign-1000.csv`).
- 1,000 SIGKILLs of a real writer process at random moments (15 to 400 ms after launch), with 128 KiB memtables and small level limits so a flush or compaction is usually in progress. An average of about 37 table files existed at each check.
- 706,905 acknowledged operations (puts, overwrites and deletes over 3,000 keys) were checked after recovery: **0 lost, 0 invented**.
- In 198 runs the operation that was in flight at the kill had also reached the log (allowed). In 23 runs the kill came before the first write.
- Only the process was killed, with the log handed to the OS on every write and **no fsync**. This shows process-crash consistency. It does not show power-loss safety, which needs `Options.Sync` and a different test.

### Mutation check: 7 of 7 caught
`python3 scripts/mutation_check.py`

| # | Injected bug | Caught by |
|---|---|---|
| 1 | manifest forgets the unflushed frozen memtable's log | differential test |
| 2 | compaction drops versions that live snapshots need | snapshot and differential tests |
| 3 | tombstones dropped while older data exists below | differential test |
| 4 | log not handed to the OS on each write | crash-simulation, torn-log and differential tests |
| 5 | old log deleted before the manifest records the new table | **crash harness only** (caught at run 7) |
| 6 | Bloom filter with false negatives | reopen, Bloom and differential tests |
| 7 | level 0 searched oldest file first | differential test |

### A real bug the tests found
Mutant 1 is not hypothetical: it was the actual first implementation. The differential test failed at operation 607 (a key returned a stale value after a restart). Cause: a compaction that finished while a memtable was frozen but not yet flushed saved a manifest saying "logs older than the active one are obsolete", which orphaned the frozen memtable's log; a crash then lost its data. Fixed by recording the log number of the oldest unflushed memtable. The same failure would eventually be found by the crash harness, but the differential test found it in under a second.

## Performance (`cmd/bench`, 1,000,000 keys, 100-byte values, one client)
Closed loop, no fsync, working set (about 120 MB) fits in the OS page cache, no compression, no block cache.

| Workload | Throughput | p50 | p99 | p99.9 | max |
|---|---|---|---|---|---|
| fillseq | 264,968 ops/s | 2.3 us | 9.4 us | 62 us | 15.1 ms |
| fillrandom | 233,099 ops/s | 2.5 us | 9.5 us | 74 us | 15.4 ms |
| readrandom (500k reads of existing keys) | 289,588 ops/s | 2.5 us | 18 us | 77 us | 21.5 ms |
| scan of 100 keys | 10,990 scans/s | | | | |

Write amplification is bytes written to disk (log + flushes + compactions) over key + value bytes the caller wrote, from the store's own counters: **4.2 for sequential fill and 6.6 for random fill** (about 1.3 of that is log framing and sequence numbers, the rest is flush and compaction rewrites). After random fill the levels held [3, 4, 25, 0, 0, 0, 0] files.

With `Options.Sync` (fsync on every write, 20,000 writes): **about 340 ops/s** (p50 2.9 ms, p99 10 ms) sequential and **about 240 ops/s** random. There is no group commit, so each write pays a full fsync; that is the cost of per-write durability here, and the obvious next optimisation.

Not measured: multi-client writes (writers are serialised), datasets larger than RAM, compression, comparison with another engine.
