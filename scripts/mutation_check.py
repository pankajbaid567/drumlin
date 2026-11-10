#!/usr/bin/env python3
"""Injects deliberate bugs into a scratch copy of Drumlin and checks that the unit/differential tests, or for
crash-only bugs the crash harness, catch every one. A surviving mutant is a hole in the tests.

  python3 scripts/mutation_check.py [--only N] [--skip-crash] [--crash-runs 400]

Every patch asserts its target text exists, so a mutant cannot silently be a no-op.
"""
import argparse, os, shutil, subprocess, sys, time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRATCH = ROOT / ".mutants"


def sub(path, old, new):
    t = path.read_text()
    if old not in t:
        raise SystemExit(f"mutation target not found in {path.name}: {old[:70]!r}")
    path.write_text(t.replace(old, new, 1))


def m1(r):  # manifest says the active log is the oldest needed one even while a frozen memtable is unflushed
    sub(r / "db/compaction.go", "	logNum := d.walNum\n	if d.imm != nil {\n		logNum = d.immWalNum\n	}\n", "	logNum := d.walNum\n")


def m2(r):  # compaction drops the older versions that live snapshots still need
    sub(r / "db/compaction.go", "	for i := 1; i < len(versions); i++ {\n		for _, s := range snaps {", "	for i := 1; i < 1; i++ {\n		for _, s := range snaps {")


def m3(r):  # tombstones dropped even when an older version lives in a deeper level: deleted keys come back
    sub(r / "db/compaction.go", "kept[0].kind == KindDel && c.isBottom(key)", "kept[0].kind == KindDel")


def m4(r):  # the log is not handed to the OS on every write: acknowledged writes can be lost by a process crash
    sub(r / "db/wal.go", "	if err := w.w.Flush(); err != nil {\n		return err\n	}\n	if w.sync {", "	if w.sync {")


def m5(r):  # the old log is deleted before the new table is recorded in the manifest (crash-only)
    sub(r / "db/compaction.go", "	d.mu.Lock()\n	if f != nil {\n		d.levels[0] = append([]*fileMeta{f}, d.levels[0]...)",
        "	os.Remove(walName(d.dir, immWal))\n	d.mu.Lock()\n	if f != nil {\n		d.levels[0] = append([]*fileMeta{f}, d.levels[0]...)")


def m6(r):  # the Bloom filter has false negatives
    sub(r / "db/bloom.go", "	h := hash32(key)\n	delta := h>>17 | h<<15\n	for i := 0; i < int(f[0]); i++ {", "	h := hash32(key)\n	if h%50 == 0 {\n		return false\n	}\n	delta := h>>17 | h<<15\n	for i := 0; i < int(f[0]); i++ {")


def m7(r):  # level 0 searched oldest file first: stale versions shadow new ones
    sub(r / "db/db.go", "	for _, f := range d.levels[0] {\n		if bytes.Compare(key, f.Smallest) < 0", "	for i0 := len(d.levels[0]) - 1; i0 >= 0; i0-- {\n		f := d.levels[0][i0]\n		if bytes.Compare(key, f.Smallest) < 0")


MUTANTS = [
    ("manifest forgets the unflushed frozen memtable's log", m1, False),
    ("compaction drops versions needed by snapshots", m2, False),
    ("tombstones dropped above older data", m3, False),
    ("log not handed to the OS on each write", m4, False),
    ("old log deleted before the manifest records the new table", m5, True),
    ("Bloom filter with false negatives", m6, False),
    ("level 0 searched oldest first", m7, False),
]


def run(cmd, cwd, timeout):
    return subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=timeout)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", type=int)
    ap.add_argument("--skip-crash", action="store_true")
    ap.add_argument("--crash-runs", type=int, default=400)
    ap.add_argument("--force-crash", action="store_true", help="judge every selected mutant by the crash harness")
    a = ap.parse_args()
    res = []
    for n, (name, patch, crash_only) in enumerate(MUTANTS, 1):
        if a.only and a.only != n:
            continue
        if crash_only and a.skip_crash:
            continue
        if SCRATCH.exists():
            shutil.rmtree(SCRATCH)
        shutil.copytree(ROOT, SCRATCH, ignore=shutil.ignore_patterns(".git", "bin", ".mutants", "results"))
        patch(SCRATCH)
        t0 = time.time()
        if crash_only or a.force_crash:
            b = run(["go", "build", "-o", "bin/crashtest", "./cmd/crashtest"], SCRATCH, 300)
            if b.returncode != 0:
                print(b.stderr[-1500:])
                raise SystemExit(f"mutant {n} does not compile")
            p = run(["./bin/crashtest", "-runs", str(a.crash_runs), "-seed", "7"], SCRATCH, 3600)
            how = "crash harness: " + next((l for l in p.stdout.splitlines() if "VIOLATION" in l), p.stdout.strip().splitlines()[-1] if p.stdout.strip() else "")[:150]
        else:
            p = run(["go", "test", "-count=1", "./db"], SCRATCH, 1800)
            fails = [l.strip() for l in p.stdout.splitlines() if l.startswith("--- FAIL")]
            how = "tests failed: " + ", ".join(f.split()[2] for f in fails[:3]) if fails else "build/test error: " + (p.stdout + p.stderr)[-200:]
        caught = p.returncode != 0
        res.append(caught)
        print(f"mutant {n}: {name}: {'CAUGHT' if caught else 'SURVIVED'} ({time.time() - t0:.0f}s) {how}", flush=True)
    if SCRATCH.exists():
        shutil.rmtree(SCRATCH)
    print(f"\n{sum(res)}/{len(res)} mutants caught")
    sys.exit(0 if all(res) else 1)


if __name__ == "__main__":
    main()
