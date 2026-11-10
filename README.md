# Drumlin

**An LSM-tree key-value store in Go, with its crash behaviour tested instead of assumed.** Skiplist memtable, CRC-framed write-ahead log, SSTables with Bloom filters and a block index, leveled compaction, snapshot reads, checksummed files, recovery. Standard library only.

Phase A (this repository, working) is the single-node store. Phase B, Raft replication with a linearizability checker, is **not started**; nothing here claims it.

## What was measured
Details, method and limits: [docs/results.md](docs/results.md). One busy M3 laptop.
- **1,000 SIGKILLs** of a real writer process at random moments, mid-flush and mid-compaction: **0 lost acknowledged writes, 0 invented ones** across 706,905 checked operations. (Process kills only; the log is handed to the OS on every write, no fsync.)
- **7 of 7 injected bugs caught**; one only by the crash harness.
- A **real bug found by the differential test** in the first implementation (a compaction orphaned an unflushed memtable's log): fixed and written up.
- A differential test against a plain map (300,000 operations with snapshots, restarts and simulated crashes) and a test that cuts the log at **every byte offset**.
- About 230,000 writes/s and 290,000 reads/s with cache-resident data and no fsync; **write amplification 4.2 (sequential) and 6.6 (random)**; with fsync on every write, about 340 writes/s (no group commit yet).

## Use it
```go
d, _ := db.Open("data", db.Options{Sync: false})
d.Put([]byte("k"), []byte("v"))
v, ok, _ := d.Get([]byte("k"))
snap := d.NewSnapshot()               // consistent view; compaction keeps what it needs
kvs, _ := d.Scan([]byte("a"), []byte("z"), 100)
snap.Release(); d.Close()
```

## Reproduce
```bash
go test -race ./...                                    # 10 tests, about a minute
go build -o bin/crashtest ./cmd/crashtest && ./bin/crashtest -runs 1000   # about 7 minutes
python3 scripts/mutation_check.py                      # 7 mutants
go build -o bin/bench ./cmd/bench && ./bin/bench -n 1000000
```

## Layout
`db/` (store), `cmd/crashtest`, `cmd/bench`, `scripts/mutation_check.py`, [docs/](docs/): [architecture](docs/architecture.md), [design decisions](docs/DESIGN.md), [testing](docs/testing.md), [results](docs/results.md), [resume claims](docs/resume-claims.md), [PRD](docs/prd.md).

## What this is not
Not production software and not a RocksDB replacement. No transactions or batches, compression, block cache, group commit, multi-process access or replication. Durability against power loss is a configuration (`Options.Sync`) that is **not covered by the crash tests**. Writers are serialised, and a long compaction can stall them (visible as the 15 ms max write latency).
