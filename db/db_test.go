package db

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func small() Options {
	return Options{MemtableBytes: 2 << 10, L0Trigger: 2, BaseLevelBytes: 8 << 10, FileBytes: 2 << 10}
}

func open(t testing.TB, dir string, o Options) *DB {
	t.Helper()
	d, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// crash stops the store the way a killed process would: no flush, no orderly shutdown of the log.
func (d *DB) crash() {
	d.writeMu.Lock()
	d.mu.Lock()
	d.closed = true
	d.mu.Unlock()
	close(d.quit)
	<-d.done
	d.wal.f.Close()
	d.closeTables()
	d.writeMu.Unlock()
}

func mustGet(t testing.TB, d *DB, k string) (string, bool) {
	t.Helper()
	v, ok, err := d.Get([]byte(k))
	if err != nil {
		t.Fatal(err)
	}
	return string(v), ok
}

func TestBasicPutGetDelete(t *testing.T) {
	d := open(t, t.TempDir(), Options{})
	defer d.Close()
	d.Put([]byte("a"), []byte("1"))
	d.Put([]byte("b"), []byte("2"))
	d.Put([]byte("a"), []byte("3"))
	d.Delete([]byte("b"))
	if v, ok := mustGet(t, d, "a"); !ok || v != "3" {
		t.Fatalf("a = %q %v", v, ok)
	}
	if _, ok := mustGet(t, d, "b"); ok {
		t.Fatal("deleted key visible")
	}
	if _, ok := mustGet(t, d, "zzz"); ok {
		t.Fatal("missing key visible")
	}
	if err := d.Put(nil, []byte("x")); err == nil {
		t.Fatal("empty key accepted")
	}
}

func TestReopenKeepsEverything(t *testing.T) {
	dir := t.TempDir()
	d := open(t, dir, small())
	for i := 0; i < 500; i++ {
		d.Put([]byte(fmt.Sprintf("k%04d", i)), bytes.Repeat([]byte{byte(i)}, 50))
	}
	d.Delete([]byte("k0007"))
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	d = open(t, dir, small())
	defer d.Close()
	for i := 0; i < 500; i++ {
		_, ok := mustGet(t, d, fmt.Sprintf("k%04d", i))
		if ok == (i == 7) {
			t.Fatalf("key %d present=%v", i, ok)
		}
	}
	if s := d.Stats(); s.Flushes == 0 && s.LevelFiles[0]+s.LevelFiles[1] == 0 {
		t.Fatal("expected tables on disk")
	}
}

func TestUnflushedWritesSurviveACrash(t *testing.T) {
	dir := t.TempDir()
	d := open(t, dir, Options{})
	for i := 0; i < 100; i++ {
		d.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("v"))
	}
	d.crash() // memtable never flushed; only the WAL has the data
	d = open(t, dir, Options{})
	defer d.Close()
	for i := 0; i < 100; i++ {
		if _, ok := mustGet(t, d, fmt.Sprintf("k%03d", i)); !ok {
			t.Fatalf("lost k%03d", i)
		}
	}
}

func TestTornWALAtEveryByteRecoversAPrefix(t *testing.T) {
	src := t.TempDir()
	d := open(t, src, Options{})
	const n = 40
	for i := 0; i < n; i++ {
		d.Put([]byte(fmt.Sprintf("k%03d", i)), []byte(fmt.Sprintf("value-%d", i)))
	}
	d.crash()
	wals, _ := filepath.Glob(filepath.Join(src, "*.wal"))
	sort.Strings(wals)
	last := wals[len(wals)-1]
	data, _ := os.ReadFile(last)
	prev := 0
	for cut := 0; cut <= len(data); cut++ {
		dst := t.TempDir()
		entries, _ := os.ReadDir(src)
		for _, e := range entries {
			b, _ := os.ReadFile(filepath.Join(src, e.Name()))
			if e.Name() == filepath.Base(last) {
				b = b[:cut]
			}
			os.WriteFile(filepath.Join(dst, e.Name()), b, 0o644)
		}
		r := open(t, dst, Options{})
		k := 0
		for i := 0; i < n; i++ {
			if _, ok := mustGet(t, r, fmt.Sprintf("k%03d", i)); ok {
				if i != k {
					t.Fatalf("cut %d: key %d present but %d missing: not a prefix", cut, i, k)
				}
				k++
			}
		}
		if k < prev {
			t.Fatalf("cut %d recovered %d records, fewer than the shorter cut (%d)", cut, k, prev)
		}
		prev = k
		r.Close()
	}
	if prev != n {
		t.Fatalf("uncut log recovered %d of %d", prev, n)
	}
}

func TestCorruptTableIsDetectedNotServed(t *testing.T) {
	dir := t.TempDir()
	d := open(t, dir, small())
	for i := 0; i < 300; i++ {
		d.Put([]byte(fmt.Sprintf("k%04d", i)), bytes.Repeat([]byte("x"), 40))
	}
	d.Close()
	ssts, _ := filepath.Glob(filepath.Join(dir, "*.sst"))
	if len(ssts) == 0 {
		t.Fatal("no tables")
	}
	b, _ := os.ReadFile(ssts[0])
	b[10] ^= 0xff // inside the first data block
	os.WriteFile(ssts[0], b, 0o644)
	d = open(t, dir, small())
	defer d.Close()
	bad := 0
	for i := 0; i < 300; i++ {
		v, ok, err := d.Get([]byte(fmt.Sprintf("k%04d", i)))
		if err != nil {
			bad++
		} else if ok && string(v) != string(bytes.Repeat([]byte("x"), 40)) {
			t.Fatalf("served wrong data for %d", i)
		}
	}
	if bad == 0 {
		t.Fatal("corruption went unnoticed")
	}
	// a damaged footer must refuse to open
	d.Close()
	b, _ = os.ReadFile(ssts[0])
	b[len(b)-5] ^= 0xff
	os.WriteFile(ssts[0], b, 0o644)
	if _, err := Open(dir, small()); err == nil {
		t.Fatal("opened a table with a bad footer")
	}
}

func TestBloomFalsePositiveRate(t *testing.T) {
	var hs []uint32
	for i := 0; i < 10000; i++ {
		hs = append(hs, hash32([]byte(fmt.Sprintf("in-%d", i))))
	}
	f := bloomBuild(hs)
	for i := 0; i < 10000; i++ {
		if !bloomMay(f, []byte(fmt.Sprintf("in-%d", i))) {
			t.Fatal("false negative")
		}
	}
	fp := 0
	for i := 0; i < 20000; i++ {
		if bloomMay(f, []byte(fmt.Sprintf("out-%d", i))) {
			fp++
		}
	}
	if rate := float64(fp) / 20000; rate > 0.03 {
		t.Fatalf("false positive rate %.3f", rate)
	}
}

func TestSnapshotSeesTheOldWorldAcrossCompaction(t *testing.T) {
	d := open(t, t.TempDir(), small())
	defer d.Close()
	for i := 0; i < 100; i++ {
		d.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("old"))
	}
	snap := d.NewSnapshot()
	for i := 0; i < 100; i++ {
		d.Put([]byte(fmt.Sprintf("k%03d", i)), []byte("new"))
		if i%2 == 0 {
			d.Delete([]byte(fmt.Sprintf("k%03d", i)))
		}
	}
	for i := 0; i < 2000; i++ { // force flushes and compactions
		d.Put([]byte(fmt.Sprintf("z%05d", i)), bytes.Repeat([]byte("p"), 30))
	}
	for i := 0; i < 100; i++ {
		v, ok, err := snap.Get([]byte(fmt.Sprintf("k%03d", i)))
		if err != nil || !ok || string(v) != "old" {
			t.Fatalf("snapshot lost k%03d: %q %v %v", i, v, ok, err)
		}
	}
	kvs, err := snap.Scan([]byte("k"), []byte("l"), 0)
	if err != nil || len(kvs) != 100 {
		t.Fatalf("snapshot scan: %d %v", len(kvs), err)
	}
	if _, ok := mustGet(t, d, "k000"); ok {
		t.Fatal("live view should see the delete")
	}
	if d.Stats().Compactions == 0 {
		t.Fatal("test did not exercise compaction")
	}
	snap.Release()
}

// The differential test applies random operations to the store and to a plain map and compares them constantly,
// including across reopen and simulated crashes, with tiny tables so flushes and compactions happen all the time.
func TestDifferentialAgainstAMap(t *testing.T) {
	ops := 20000
	if os.Getenv("DRUMLIN_LONG") != "" {
		ops = 300000
	}
	rnd := rand.New(rand.NewSource(99))
	dir := t.TempDir()
	d := open(t, dir, small())
	model := map[string]string{}
	type snapState struct {
		s *Snapshot
		m map[string]string
	}
	var snaps []snapState
	key := func() string { return fmt.Sprintf("k%03d", rnd.Intn(300)) }
	check := func() {
		var keys []string
		for k := range model {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		kvs, err := d.Scan(nil, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(kvs) != len(keys) {
			t.Fatalf("scan has %d keys, model %d", len(kvs), len(keys))
		}
		for i, kv := range kvs {
			if string(kv.Key) != keys[i] || string(kv.Value) != model[keys[i]] {
				t.Fatalf("scan mismatch at %s", keys[i])
			}
		}
	}
	for i := 0; i < ops; i++ {
		switch r := rnd.Intn(100); {
		case r < 55:
			k, v := key(), fmt.Sprintf("v%d-%s", i, bytes.Repeat([]byte("x"), rnd.Intn(80)))
			if err := d.Put([]byte(k), []byte(v)); err != nil {
				t.Fatal(err)
			}
			model[k] = v
		case r < 70:
			k := key()
			d.Delete([]byte(k))
			delete(model, k)
		case r < 90:
			k := key()
			v, ok := mustGet(t, d, k)
			if mv, mok := model[k]; ok != mok || v != mv {
				t.Fatalf("op %d: get %s = %q/%v, model %q/%v\n%s", i, k, v, ok, mv, mok, d.dumpKey(k))
			}
		case r < 93:
			c := map[string]string{}
			for k, v := range model {
				c[k] = v
			}
			snaps = append(snaps, snapState{d.NewSnapshot(), c})
		case r < 95 && len(snaps) > 0:
			j := rnd.Intn(len(snaps))
			k := key()
			v, ok, err := snaps[j].s.Get([]byte(k))
			if mv, mok := snaps[j].m[k]; err != nil || ok != mok || string(v) != mv {
				t.Fatalf("op %d: snapshot(seq %d) get %s = %q/%v, want %q/%v (%v)\n%s", i, snaps[j].s.seq, k, v, ok, mv, mok, err, d.dumpKey(k))
			}
			if rnd.Intn(3) == 0 {
				snaps[j].s.Release()
				snaps = append(snaps[:j], snaps[j+1:]...)
			}
		case r < 96:
			for _, s := range snaps {
				s.s.Release()
			}
			snaps = nil
			if rnd.Intn(2) == 0 {
				d.crash()
			} else if err := d.Close(); err != nil {
				t.Fatal(err)
			}
			d = open(t, dir, small())
			check() // everything must be exactly as before the restart
		default:
			if i%200 == 0 {
				check()
			}
		}
	}
	check()
	for _, s := range snaps {
		s.s.Release()
	}
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentReadersAndWriter(t *testing.T) {
	d := open(t, t.TempDir(), small())
	defer d.Close()
	done := make(chan struct{})
	errc := make(chan error, 4)
	for g := 0; g < 4; g++ {
		go func(g int) {
			r := rand.New(rand.NewSource(int64(g)))
			for {
				select {
				case <-done:
					errc <- nil
					return
				default:
				}
				k := fmt.Sprintf("k%03d", r.Intn(200))
				if v, ok, err := d.Get([]byte(k)); err != nil {
					errc <- err
					return
				} else if ok && !bytes.HasPrefix(v, []byte("v")) {
					errc <- fmt.Errorf("bad value %q", v)
					return
				}
				if _, err := d.Scan([]byte("k000"), []byte("k050"), 20); err != nil {
					errc <- err
					return
				}
			}
		}(g)
	}
	for i := 0; i < 6000; i++ {
		d.Put([]byte(fmt.Sprintf("k%03d", i%200)), []byte(fmt.Sprintf("v%d", i)))
	}
	close(done)
	for g := 0; g < 4; g++ {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}
}

func TestWriteAmplificationIsReported(t *testing.T) {
	d := open(t, t.TempDir(), small())
	defer d.Close()
	for i := 0; i < 5000; i++ {
		d.Put([]byte(fmt.Sprintf("k%05d", rand.Intn(100000))), bytes.Repeat([]byte("v"), 100))
	}
	s := d.Stats()
	if s.UserBytes == 0 || s.WriteAmplification() < 1 {
		t.Fatalf("implausible stats %+v", s)
	}
}
