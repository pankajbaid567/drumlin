// Package db is an LSM-tree key-value store: a skiplist memtable, a CRC-framed write-ahead log, immutable SSTables
// with block index and Bloom filters, leveled compaction, snapshot reads and crash recovery.
package db

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Options tune the store. The zero value is usable.
type Options struct {
	MemtableBytes  int64 // flush the memtable at this size (default 4 MiB)
	L0Trigger      int   // compact level 0 when it has this many files (default 4)
	BaseLevelBytes int64 // size limit of level 1; each deeper level is 10x (default 16 MiB)
	FileBytes      int64 // target size of compaction output files (default 4 MiB)
	Sync           bool  // fsync the WAL on every write (power-loss durability); off still survives a process crash
	MaxLevels      int   // default 7
}

func (o *Options) fill() {
	if o.MemtableBytes <= 0 {
		o.MemtableBytes = 4 << 20
	}
	if o.L0Trigger <= 0 {
		o.L0Trigger = 4
	}
	if o.BaseLevelBytes <= 0 {
		o.BaseLevelBytes = 16 << 20
	}
	if o.FileBytes <= 0 {
		o.FileBytes = 4 << 20
	}
	if o.MaxLevels <= 0 {
		o.MaxLevels = 7
	}
}

var ErrClosed = errors.New("drumlin: closed")

// Stats counts bytes written so write amplification can be reported.
type Stats struct {
	UserBytes       int64 // key + value bytes the caller wrote
	WALBytes        int64
	FlushBytes      int64
	CompactionBytes int64 // bytes written by compactions
	Compactions     int64
	Flushes         int64
	LevelFiles      []int
}

// WriteAmplification is total bytes written to disk (WAL + flushes + compactions) per user byte.
func (s Stats) WriteAmplification() float64 {
	if s.UserBytes == 0 {
		return 0
	}
	return float64(s.WALBytes+s.FlushBytes+s.CompactionBytes) / float64(s.UserBytes)
}

type DB struct {
	dir  string
	opts Options

	writeMu sync.Mutex // serialises writers
	mu      sync.RWMutex
	cond    *sync.Cond // on mu

	mem, imm   *skiplist
	wal        *walWriter
	walNum     uint64
	immWalNum  uint64
	levels     [][]*fileMeta // level 0: newest first; deeper levels: sorted by key, non-overlapping
	seq        uint64
	nextFile   uint64
	snaps      map[uint64]int
	closed     bool
	bgErr      error
	compactPtr map[int][]byte

	wake chan struct{}
	quit chan struct{}
	done chan struct{}

	userBytes, walBytes, flushBytes, compBytes, nComp, nFlush atomic.Int64
}

func walName(dir string, n uint64) string { return filepath.Join(dir, fmt.Sprintf("%08d.wal", n)) }
func sstName(dir string, n uint64) string { return filepath.Join(dir, fmt.Sprintf("%08d.sst", n)) }

// Open opens or creates a store in dir, recovering from any earlier crash.
func Open(dir string, opts Options) (*DB, error) {
	opts.fill()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	d := &DB{dir: dir, opts: opts, mem: newSkiplist(), snaps: map[uint64]int{}, compactPtr: map[int][]byte{},
		wake: make(chan struct{}, 1), quit: make(chan struct{}), done: make(chan struct{})}
	d.cond = sync.NewCond(&d.mu)
	d.levels = make([][]*fileMeta, opts.MaxLevels)
	m, err := loadManifest(dir)
	if err != nil {
		return nil, fmt.Errorf("drumlin: manifest: %w", err)
	}
	var logNum uint64
	if m != nil {
		d.seq, d.nextFile, logNum = m.LastSeq, m.NextFile, m.LogNum
		for l, files := range m.Levels {
			if l >= opts.MaxLevels {
				return nil, errors.New("drumlin: manifest has more levels than MaxLevels")
			}
			for _, f := range files {
				t, err := openTable(sstName(dir, f.Num), f.Num)
				if err != nil {
					d.closeTables()
					return nil, err
				}
				f.t = t
				d.levels[l] = append(d.levels[l], f)
			}
		}
	}
	if d.nextFile == 0 {
		d.nextFile = 1
	}
	// replay WALs that were not yet flushed
	names, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	var nums []uint64
	for _, n := range names {
		var num uint64
		if _, err := fmt.Sscanf(filepath.Base(n), "%d.wal", &num); err == nil && num >= logNum {
			nums = append(nums, num)
		}
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	for i, num := range nums {
		es, _, torn, err := readWAL(walName(dir, num))
		if err != nil {
			d.closeTables()
			return nil, err
		}
		if torn && i != len(nums)-1 {
			d.closeTables()
			return nil, fmt.Errorf("drumlin: corruption inside %s (only the newest log may have a torn tail)", walName(dir, num))
		}
		for _, e := range es {
			d.mem.insert(e)
			if e.seq > d.seq {
				d.seq = e.seq
			}
		}
	}
	d.walNum = d.allocFile()
	if d.mem.n > 0 {
		if err := d.writeLevel0(d.mem); err != nil {
			d.closeTables()
			return nil, err
		}
		d.mem = newSkiplist()
	}
	if err := d.saveManifest(d.walNum); err != nil {
		d.closeTables()
		return nil, err
	}
	d.wal, err = createWAL(walName(dir, d.walNum), opts.Sync)
	if err != nil {
		d.closeTables()
		return nil, err
	}
	d.cleanup()
	go d.background()
	return d, nil
}

func (d *DB) allocFile() uint64 {
	n := d.nextFile
	d.nextFile++
	return n
}

func (d *DB) saveManifest(logNum uint64) error {
	m := &manifest{NextFile: d.nextFile, LastSeq: d.seq, LogNum: logNum, Levels: d.levels}
	return writeManifest(d.dir, m)
}

// cleanup removes files the manifest does not reference: leftovers of an interrupted flush or compaction.
func (d *DB) cleanup() {
	live := map[string]bool{}
	for _, l := range d.levels {
		for _, f := range l {
			live[filepath.Base(sstName(d.dir, f.Num))] = true
		}
	}
	entries, _ := os.ReadDir(d.dir)
	for _, e := range entries {
		n := e.Name()
		switch {
		case strings.HasSuffix(n, ".tmp"):
			os.Remove(filepath.Join(d.dir, n))
		case strings.HasSuffix(n, ".sst") && !live[n]:
			os.Remove(filepath.Join(d.dir, n))
		case strings.HasSuffix(n, ".wal"):
			var num uint64
			if _, err := fmt.Sscanf(n, "%d.wal", &num); err == nil && num < d.walNum {
				os.Remove(filepath.Join(d.dir, n))
			}
		}
	}
}

func (d *DB) closeTables() {
	for _, l := range d.levels {
		for _, f := range l {
			if f.t != nil {
				f.t.f.Close()
			}
		}
	}
}

// writeLevel0 writes a memtable as a new level-0 table and records it. Used at open (no other goroutines yet).
func (d *DB) writeLevel0(s *skiplist) error {
	f, err := d.buildTable(s.iter(), d.allocFile())
	if err != nil || f == nil {
		return err
	}
	d.levels[0] = append([]*fileMeta{f}, d.levels[0]...)
	d.flushBytes.Add(f.Size)
	d.nFlush.Add(1)
	return nil
}

func (d *DB) buildTable(it iterator, num uint64) (*fileMeta, error) {
	if !it.Valid() {
		return nil, it.Err()
	}
	path := sstName(d.dir, num)
	w, err := newTableWriter(path)
	if err != nil {
		return nil, err
	}
	for ; it.Valid(); it.Next() {
		if err := w.add(it.Entry()); err != nil {
			w.abort()
			os.Remove(path)
			return nil, err
		}
	}
	if err := it.Err(); err != nil {
		w.abort()
		os.Remove(path)
		return nil, err
	}
	size, err := w.finish()
	if err != nil {
		os.Remove(path)
		return nil, err
	}
	t, err := openTable(path, num)
	if err != nil {
		return nil, err
	}
	return &fileMeta{Num: num, Size: size, Smallest: w.smallest, Largest: append([]byte(nil), w.largest...), t: t}, nil
}

// ---------------------------------------------------------------------------------------------- writes

func (d *DB) Put(key, value []byte) error { return d.write(key, value, KindPut) }
func (d *DB) Delete(key []byte) error     { return d.write(key, nil, KindDel) }

func (d *DB) write(key, value []byte, kind Kind) error {
	if len(key) == 0 || len(key) > 1<<16 || len(value) > 16<<20 {
		return errors.New("drumlin: key must be 1-65536 bytes and value at most 16 MiB")
	}
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	if err := d.rotateIfFull(); err != nil {
		return err
	}
	seq := d.seq + 1
	e := entry{key: append([]byte(nil), key...), seq: seq, kind: kind, value: append([]byte(nil), value...)}
	before := d.wal.bytes
	if err := d.wal.append(e); err != nil {
		return err
	}
	d.walBytes.Add(d.wal.bytes - before)
	d.userBytes.Add(int64(len(key) + len(value)))
	d.mu.Lock()
	d.mem.insert(e)
	d.seq = seq
	d.mu.Unlock()
	return nil
}

// rotateIfFull freezes a full memtable and starts a new WAL. Caller holds writeMu. It stalls while the previous
// frozen memtable is still being flushed.
func (d *DB) rotateIfFull() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	if d.bgErr != nil {
		return d.bgErr
	}
	if d.mem.size < d.opts.MemtableBytes {
		return nil
	}
	return d.rotateLocked()
}

func (d *DB) rotateLocked() error {
	for d.imm != nil && d.bgErr == nil {
		d.cond.Wait()
	}
	if d.bgErr != nil {
		return d.bgErr
	}
	if err := d.wal.close(); err != nil { // fsync the finished log before relying on a newer one
		return err
	}
	d.imm, d.immWalNum = d.mem, d.walNum
	d.mem = newSkiplist()
	d.walNum = d.allocFile()
	w, err := createWAL(walName(d.dir, d.walNum), d.opts.Sync)
	if err != nil {
		d.bgErr = err
		return err
	}
	d.wal = w
	select {
	case d.wake <- struct{}{}:
	default:
	}
	return nil
}

// ---------------------------------------------------------------------------------------------- reads

// Get returns the value for key, or ok=false if it is absent or deleted.
func (d *DB) Get(key []byte) (value []byte, ok bool, err error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return nil, false, ErrClosed
	}
	return d.getLocked(key, d.seq)
}

func (d *DB) getLocked(key []byte, maxSeq uint64) ([]byte, bool, error) {
	for _, m := range []*skiplist{d.mem, d.imm} {
		if m == nil {
			continue
		}
		if n := m.seek(key, maxSeq); n != nil && bytes.Equal(n.e.key, key) {
			return result(n.e)
		}
	}
	for _, f := range d.levels[0] {
		if bytes.Compare(key, f.Smallest) < 0 || bytes.Compare(key, f.Largest) > 0 {
			continue
		}
		e, ok, err := f.t.get(key, maxSeq)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return result(e)
		}
	}
	for l := 1; l < len(d.levels); l++ {
		files := d.levels[l]
		i := sort.Search(len(files), func(i int) bool { return bytes.Compare(files[i].Largest, key) >= 0 })
		if i == len(files) || bytes.Compare(key, files[i].Smallest) < 0 {
			continue
		}
		e, ok, err := files[i].t.get(key, maxSeq)
		if err != nil {
			return nil, false, err
		}
		if ok {
			return result(e)
		}
	}
	return nil, false, nil
}

func result(e entry) ([]byte, bool, error) {
	if e.kind == KindDel {
		return nil, false, nil
	}
	return append([]byte(nil), e.value...), true, nil
}

// Scan returns live keys in [start, end) in order, at most limit of them (0 = no limit). A nil end means no upper bound.
func (d *DB) Scan(start, end []byte, limit int) ([]KV, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.closed {
		return nil, ErrClosed
	}
	return d.scanLocked(start, end, limit, d.seq)
}

func (d *DB) scanLocked(start, end []byte, limit int, maxSeq uint64) ([]KV, error) {
	var its []iterator
	its = append(its, d.mem.iterFrom(start))
	if d.imm != nil {
		its = append(its, d.imm.iterFrom(start))
	}
	for _, l := range d.levels {
		for _, f := range l {
			if bytes.Compare(f.Largest, start) < 0 || (end != nil && bytes.Compare(f.Smallest, end) >= 0) {
				continue
			}
			its = append(its, f.t.iterFrom(start))
		}
	}
	m := newMerge(its)
	var out []KV
	var last []byte
	haveLast := false
	for ; m.Valid(); m.Next() {
		e := m.Entry()
		if end != nil && bytes.Compare(e.key, end) >= 0 {
			break
		}
		if e.seq > maxSeq || (haveLast && bytes.Equal(e.key, last)) {
			continue
		}
		last, haveLast = e.key, true
		if e.kind == KindPut {
			out = append(out, KV{Key: append([]byte(nil), e.key...), Value: append([]byte(nil), e.value...)})
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	return out, m.Err()
}

// Snapshot is a consistent read view. Compaction keeps the versions a live snapshot needs; call Release when done.
type Snapshot struct {
	d   *DB
	seq uint64
}

func (d *DB) NewSnapshot() *Snapshot {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.snaps[d.seq]++
	return &Snapshot{d: d, seq: d.seq}
}

func (s *Snapshot) Get(key []byte) ([]byte, bool, error) {
	s.d.mu.RLock()
	defer s.d.mu.RUnlock()
	if s.d.closed {
		return nil, false, ErrClosed
	}
	return s.d.getLocked(key, s.seq)
}

func (s *Snapshot) Scan(start, end []byte, limit int) ([]KV, error) {
	s.d.mu.RLock()
	defer s.d.mu.RUnlock()
	if s.d.closed {
		return nil, ErrClosed
	}
	return s.d.scanLocked(start, end, limit, s.seq)
}

func (s *Snapshot) Release() {
	s.d.mu.Lock()
	defer s.d.mu.Unlock()
	if s.d.snaps[s.seq]--; s.d.snaps[s.seq] <= 0 {
		delete(s.d.snaps, s.seq)
	}
}

// ---------------------------------------------------------------------------------------------- lifecycle

func (d *DB) Stats() Stats {
	d.mu.RLock()
	defer d.mu.RUnlock()
	s := Stats{UserBytes: d.userBytes.Load(), WALBytes: d.walBytes.Load(), FlushBytes: d.flushBytes.Load(),
		CompactionBytes: d.compBytes.Load(), Compactions: d.nComp.Load(), Flushes: d.nFlush.Load()}
	for _, l := range d.levels {
		s.LevelFiles = append(s.LevelFiles, len(l))
	}
	return s
}

// Close flushes the memtable and stops background work. Skipping Close (a crash) is safe: the WAL is replayed on Open.
func (d *DB) Close() error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return nil
	}
	var err error
	if d.mem.n > 0 && d.bgErr == nil {
		err = d.rotateLocked()
	}
	for d.imm != nil && d.bgErr == nil {
		d.cond.Wait()
	}
	if d.bgErr != nil && err == nil {
		err = d.bgErr
	}
	d.closed = true
	d.mu.Unlock()
	close(d.quit)
	<-d.done
	if e := d.wal.close(); e != nil && err == nil {
		err = e
	}
	d.closeTables()
	return err
}
