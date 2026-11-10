# Testing

| Layer | What | Where |
|---|---|---|
| Unit | put/get/delete, reopen, Bloom filter false-positive rate, WAL crash survival | `db/db_test.go` |
| Torn log | the newest log cut at **every byte offset**: recovery must yield a prefix of the writes, never a hole, and never fewer records for a longer cut | `TestTornWALAtEveryByteRecoversAPrefix` |
| Corruption | a flipped bit in a data block returns an error and never wrong data; a damaged footer refuses to open | `TestCorruptTableIsDetectedNotServed` |
| Snapshots | a snapshot taken before overwrites, deletes and 2,000 more writes (so flushes and compactions) still reads the old world | `TestSnapshotSeesTheOldWorldAcrossCompaction` |
| Differential | random puts, deletes, gets, snapshots, reopens and simulated crashes against a plain `map`, with tiny tables so flushes and compactions run constantly; full scan compared against the model after every restart | `TestDifferentialAgainstAMap` (20,000 ops by default; `DRUMLIN_LONG=1` runs 300,000) |
| Concurrency | 4 readers and scanners against a writer, under the race detector | `TestConcurrentReadersAndWriter` |
| Crash harness | real process, SIGKILL at a random moment, recovery verified on a copy of the crashed directory | `cmd/crashtest` |
| Mutation | 7 injected bugs, each must be caught | `scripts/mutation_check.py` |

The crash harness applies a deterministic sequence of operations one at a time and prints each operation's number after the store acknowledges it. After a kill the store must equal the state after operation N, where N is the last acknowledged operation or the one after it (the in-flight operation may or may not have reached the log). Anything else is a violation. Verification runs on a copy so the next writer starts from the genuine crashed state, including kills during recovery, flush and compaction.

What none of this covers: power loss or disk faults (only processes are killed), concurrent writers (the store serialises them), and long-duration soak behaviour.
