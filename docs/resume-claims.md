# Resume claims register (phase A only)

| Claim | Proof command | Status |
|---|---|---|
| LSM-tree key-value store in Go: WAL, SSTables with Bloom filters and block index, leveled compaction, snapshot reads | `go test -race ./...` | verified |
| 0 lost acknowledged writes over 1,000 SIGKILL recovery tests (706,905 operations checked) | `go build -o bin/crashtest ./cmd/crashtest && ./bin/crashtest -runs 1000` (about 7 minutes) | verified once, 2026-11-10; process crashes only, no fsync |
| Differential test against a reference map including snapshots and crashes (300,000 operations) | `DRUMLIN_LONG=1 go test -run Differential ./db` | verified |
| Found and fixed a WAL-retention bug that lost data after a crash during compaction | `docs/results.md` | verified |
| Test suite catches 7 of 7 injected bugs | `python3 scripts/mutation_check.py` | verified |
| About 230k writes/s and 290k reads/s, write amplification 4 to 7, single client, cache-resident data | `./bin/bench -n 1000000` | verified, busy laptop |

Do not claim: replication or Raft (not started), power-loss durability tested, production use, comparison with RocksDB or LevelDB, anything about datasets larger than memory.
