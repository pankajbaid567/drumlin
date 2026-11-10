# PRD

**Goal.** A small LSM-tree key-value store that is correct under crashes and whose behaviour (write amplification, tail latency) is measured rather than asserted.
**Audience.** Storage and infrastructure interviewers; the evidence is the crash and differential tests, not the feature list.
**In scope (phase A).** Put, Get, Delete, ordered Scan, snapshots, WAL, SSTables with Bloom filters and block index, leveled compaction, crash recovery, checksums, benchmarks with write amplification.
**Out of scope.** Transactions, SQL, compression, multi-process access, production claims. Replication (phase B) is not started.
**Success criteria.** 1,000 random kills with 0 lost acknowledged writes; a differential test against a plain map with snapshots, reopen and simulated crashes; a mutation check where every injected bug is caught; numbers reported with method and hardware.
