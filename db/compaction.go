package db

import (
	"bytes"
	"os"
	"sort"
)

type compaction struct {
	level  int
	in0    []*fileMeta // from level
	in1    []*fileMeta // overlapping files of level+1
	deeper [][]*fileMeta
}

func (d *DB) background() {
	defer close(d.done)
	select { // pick up work left over from before a restart
	case d.wake <- struct{}{}:
	default:
	}
	for {
		select {
		case <-d.wake:
		case <-d.quit:
			return
		}
		for {
			did, err := d.step()
			if err != nil {
				d.mu.Lock()
				d.bgErr = err
				d.cond.Broadcast()
				d.mu.Unlock()
				return
			}
			if !did {
				break
			}
			select {
			case <-d.quit:
				return
			default:
			}
		}
	}
}

func (d *DB) step() (bool, error) {
	d.mu.RLock()
	imm, immWal := d.imm, d.immWalNum
	d.mu.RUnlock()
	if imm != nil {
		return true, d.flush(imm, immWal)
	}
	if c := d.pick(); c != nil {
		return true, d.compact(c)
	}
	return false, nil
}

func (d *DB) flush(imm *skiplist, immWal uint64) error {
	d.mu.Lock()
	num := d.allocFile()
	d.mu.Unlock()
	f, err := d.buildTable(imm.iter(), num)
	if err != nil {
		return err
	}
	d.mu.Lock()
	if f != nil {
		d.levels[0] = append([]*fileMeta{f}, d.levels[0]...)
		d.flushBytes.Add(f.Size)
		d.nFlush.Add(1)
	}
	err = d.saveManifest(d.walNum) // from here on, logs older than the active one are obsolete
	if err == nil {
		d.imm = nil
	}
	d.cond.Broadcast()
	d.mu.Unlock()
	if err == nil {
		os.Remove(walName(d.dir, immWal))
	}
	return err
}

func (d *DB) maxBytes(level int) int64 {
	b := d.opts.BaseLevelBytes
	for i := 1; i < level; i++ {
		b *= 10
	}
	return b
}

func keyRange(fs []*fileMeta) (lo, hi []byte) {
	for i, f := range fs {
		if i == 0 || bytes.Compare(f.Smallest, lo) < 0 {
			lo = f.Smallest
		}
		if i == 0 || bytes.Compare(f.Largest, hi) > 0 {
			hi = f.Largest
		}
	}
	return
}

func overlapping(fs []*fileMeta, lo, hi []byte) []*fileMeta {
	var out []*fileMeta
	for _, f := range fs {
		if bytes.Compare(f.Largest, lo) >= 0 && bytes.Compare(f.Smallest, hi) <= 0 {
			out = append(out, f)
		}
	}
	return out
}

func (d *DB) pick() *compaction {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var c *compaction
	if len(d.levels[0]) >= d.opts.L0Trigger && len(d.levels) > 1 {
		c = &compaction{level: 0, in0: append([]*fileMeta(nil), d.levels[0]...)}
	} else {
		for l := 1; l < len(d.levels)-1 && c == nil; l++ {
			var total int64
			for _, f := range d.levels[l] {
				total += f.Size
			}
			if total <= d.maxBytes(l) {
				continue
			}
			pick := d.levels[l][0]
			for _, f := range d.levels[l] {
				if bytes.Compare(f.Smallest, d.compactPtr[l]) > 0 {
					pick = f
					break
				}
			}
			d.compactPtr[l] = pick.Largest
			c = &compaction{level: l, in0: []*fileMeta{pick}}
		}
	}
	if c == nil {
		return nil
	}
	lo, hi := keyRange(c.in0)
	c.in1 = overlapping(d.levels[c.level+1], lo, hi)
	for l := c.level + 2; l < len(d.levels); l++ {
		c.deeper = append(c.deeper, append([]*fileMeta(nil), d.levels[l]...))
	}
	return c
}

// keep decides which versions of one user key (newest first) survive: the newest always, and any older version
// that is the newest one visible to some live snapshot.
func keep(versions []entry, snaps []uint64) []entry {
	out := []entry{versions[0]}
	for i := 1; i < len(versions); i++ {
		for _, s := range snaps {
			if versions[i].seq <= s && s < versions[i-1].seq {
				out = append(out, versions[i])
				break
			}
		}
	}
	return out
}

func (c *compaction) isBottom(key []byte) bool {
	for _, l := range c.deeper {
		for _, f := range l {
			if bytes.Compare(key, f.Smallest) >= 0 && bytes.Compare(key, f.Largest) <= 0 {
				return false
			}
		}
	}
	return true
}

func (d *DB) compact(c *compaction) error {
	d.mu.RLock()
	var snaps []uint64
	for s := range d.snaps {
		snaps = append(snaps, s)
	}
	d.mu.RUnlock()
	sort.Slice(snaps, func(i, j int) bool { return snaps[i] < snaps[j] })

	var its []iterator
	for _, f := range append(append([]*fileMeta(nil), c.in0...), c.in1...) {
		its = append(its, f.t.iter())
	}
	m := newMerge(its)
	var outs []*fileMeta
	var w *tableWriter
	var num uint64
	finish := func() error {
		if w == nil {
			return nil
		}
		smallest, largest := w.smallest, append([]byte(nil), w.largest...)
		size, err := w.finish()
		if err != nil {
			return err
		}
		t, err := openTable(sstName(d.dir, num), num)
		if err != nil {
			return err
		}
		outs = append(outs, &fileMeta{Num: num, Size: size, Smallest: smallest, Largest: largest, t: t})
		w = nil
		return nil
	}
	cleanupOuts := func() {
		if w != nil {
			w.abort()
			os.Remove(sstName(d.dir, num))
		}
		for _, o := range outs {
			o.t.f.Close()
			os.Remove(sstName(d.dir, o.Num))
		}
	}
	for m.Valid() {
		key := append([]byte(nil), m.Entry().key...)
		var versions []entry
		for m.Valid() && bytes.Equal(m.Entry().key, key) {
			versions = append(versions, m.Entry())
			m.Next()
		}
		kept := keep(versions, snaps)
		if len(kept) == 1 && kept[0].kind == KindDel && c.isBottom(key) {
			continue // nothing older can resurface, so the tombstone has done its job
		}
		if w == nil {
			d.mu.Lock()
			num = d.allocFile()
			d.mu.Unlock()
			var err error
			if w, err = newTableWriter(sstName(d.dir, num)); err != nil {
				cleanupOuts()
				return err
			}
		}
		for _, e := range kept {
			if err := w.add(e); err != nil {
				cleanupOuts()
				return err
			}
		}
		if w.off+int64(w.block.Len()) >= d.opts.FileBytes { // cut only between user keys
			if err := finish(); err != nil {
				cleanupOuts()
				return err
			}
		}
	}
	if err := m.Err(); err != nil {
		cleanupOuts()
		return err
	}
	if err := finish(); err != nil {
		cleanupOuts()
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	remove := map[uint64]bool{}
	for _, f := range c.in0 {
		remove[f.Num] = true
	}
	for _, f := range c.in1 {
		remove[f.Num] = true
	}
	strip := func(fs []*fileMeta) []*fileMeta {
		var out []*fileMeta
		for _, f := range fs {
			if !remove[f.Num] {
				out = append(out, f)
			}
		}
		return out
	}
	d.levels[c.level] = strip(d.levels[c.level])
	next := append(strip(d.levels[c.level+1]), outs...)
	sort.Slice(next, func(i, j int) bool { return bytes.Compare(next[i].Smallest, next[j].Smallest) < 0 })
	d.levels[c.level+1] = next
	// A memtable may have been frozen while this compaction ran: its log is still the only copy of its data.
	logNum := d.walNum
	if d.imm != nil {
		logNum = d.immWalNum
	}
	if err := d.saveManifest(logNum); err != nil {
		return err
	}
	var written int64
	for _, o := range outs {
		written += o.Size
	}
	d.compBytes.Add(written)
	d.nComp.Add(1)
	for _, f := range append(append([]*fileMeta(nil), c.in0...), c.in1...) {
		f.t.f.Close()
		os.Remove(sstName(d.dir, f.Num))
	}
	return nil
}
