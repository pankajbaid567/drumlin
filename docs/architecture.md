# Architecture

Single process, single directory. Everything below is in package `db`.

```
 Put/Delete                                  Get / Scan / Snapshot
     |                                              |
     v                                              v
  [WAL]  append record, hand to the OS     memtable -> frozen memtable -> L0 files (newest first)
     |   (fsync too if Options.Sync)                                    -> L1 -> L2 ... (sorted, non-overlapping)
     v
  [memtable: skiplist]  --full--> frozen --flush--> L0 SSTable --compaction--> deeper levels
```

## Write path
1. One writer at a time (`writeMu`). If the memtable is full, freeze it, fsync and close its log, start a new log. If the previous frozen memtable is still flushing, the writer waits (write stall).
2. Assign the next sequence number, append `seq | kind | key | value` to the log in a CRC32C frame, flush to the OS (and fsync if `Sync`). Only then insert into the memtable and acknowledge.

## Files
- `NNNNNNNN.wal`: log. Record = `len u32 | crc32c u32 | payload`. A bad or short record at the end of the **newest** log is a torn tail and is discarded; the same damage anywhere else is corruption and `Open` fails.
- `NNNNNNNN.sst`: sorted table. 4 KiB data blocks each followed by a CRC32C; a Bloom filter (10 bits per key, 7 probes); a block index; a 52-byte footer with a magic number and CRC. A flipped bit makes the read return an error, never wrong data.
- `MANIFEST`: JSON listing the live tables per level, `next_file`, `last_seq` and `log_num` (logs numbered below it are obsolete). Replaced atomically: temp file, fsync, rename, fsync of the directory.

## Versions and snapshots
Every write gets a sequence number. Entries are ordered by key ascending, then sequence descending. A read at sequence S returns the newest version with seq <= S. A snapshot is just a remembered S; compaction keeps, for each key, the newest version and every older version that some live snapshot still needs, and drops a tombstone only when nothing older exists below it.

## Compaction (leveled)
- L0 files may overlap each other. At `L0Trigger` files, all of L0 plus the overlapping L1 files are merged into L1.
- Level N >= 1 holds sorted, non-overlapping files up to `BaseLevelBytes * 10^(N-1)`. When over, one file (round robin by key) and the overlapping files of the next level are merged down.
- Output files are cut only between user keys, so all versions of a key stay in one file and a point read consults at most one file per level >= 1.
- One background goroutine does flushes and compactions. Readers hold a read lock while they read files; the swap that installs a compaction's result and deletes the inputs takes the write lock, so a reader never loses a file under it.

## Recovery
`Open` reads the manifest, opens the listed tables, replays every log numbered >= `log_num` into a memtable, writes that memtable out as an L0 table, saves the manifest, starts a fresh log, and deletes files the manifest does not reference (leftovers of an interrupted flush or compaction).

## Not here
Transactions or batches, a block cache (the OS page cache does the caching), compression, group commit (each write is its own log append), range deletes, multi-process access. Replication (Raft) is phase B and has not been started.
