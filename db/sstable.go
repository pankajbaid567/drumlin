package db

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
)

// SSTable file layout:
//
//	data blocks   each ~4 KiB of entries, followed by a CRC32C of the block
//	bloom filter  followed by its CRC32C
//	index         one record per block (first key, first seq, offset, length), followed by its CRC32C
//	footer (52 B) indexOff, indexLen, bloomOff, bloomLen, entries (u64 each) | magic u64 | CRC32C of the first 48 bytes
//
// Entry: klen uvarint | key | seq u64 | kind u8 | vlen uvarint | value.
const (
	blockSize  = 4096
	footerSize = 52
	tableMagic = 0x4452554d4c494e31 // "DRUMLIN1"
	maxEntrySz = 64 << 20
)

var errCorrupt = errors.New("drumlin: corrupt sstable")

type indexEntry struct {
	key    []byte
	seq    uint64
	off    int64
	length uint32 // including the trailing CRC
}

type tableWriter struct {
	f        *os.File
	w        *bufio.Writer
	off      int64
	block    bytes.Buffer
	first    entry
	index    []indexEntry
	hashes   []uint32
	lastKey  []byte
	n        int64
	smallest []byte
	largest  []byte
}

func newTableWriter(path string) (*tableWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &tableWriter{f: f, w: bufio.NewWriterSize(f, 256<<10)}, nil
}

func (t *tableWriter) add(e entry) error {
	if t.block.Len() == 0 {
		t.first = entry{key: append([]byte(nil), e.key...), seq: e.seq}
	}
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], uint64(len(e.key)))
	t.block.Write(tmp[:n])
	t.block.Write(e.key)
	binary.Write(&t.block, binary.LittleEndian, e.seq)
	t.block.WriteByte(byte(e.kind))
	n = binary.PutUvarint(tmp[:], uint64(len(e.value)))
	t.block.Write(tmp[:n])
	t.block.Write(e.value)
	if !bytes.Equal(e.key, t.lastKey) {
		t.hashes = append(t.hashes, hash32(e.key))
		t.lastKey = append(t.lastKey[:0], e.key...)
	}
	if t.smallest == nil {
		t.smallest = append([]byte(nil), e.key...)
	}
	t.largest = append(t.largest[:0], e.key...)
	t.n++
	if t.block.Len() >= blockSize {
		return t.flushBlock()
	}
	return nil
}

func (t *tableWriter) writeChecked(b []byte) (off int64, length uint32, err error) {
	off = t.off
	var c [4]byte
	binary.LittleEndian.PutUint32(c[:], crc32.Checksum(b, castagnoli))
	if _, err = t.w.Write(b); err != nil {
		return
	}
	if _, err = t.w.Write(c[:]); err != nil {
		return
	}
	length = uint32(len(b) + 4)
	t.off += int64(length)
	return
}

func (t *tableWriter) flushBlock() error {
	if t.block.Len() == 0 {
		return nil
	}
	off, l, err := t.writeChecked(t.block.Bytes())
	if err != nil {
		return err
	}
	t.index = append(t.index, indexEntry{key: t.first.key, seq: t.first.seq, off: off, length: l})
	t.block.Reset()
	return nil
}

// finish writes bloom, index and footer, fsyncs and closes. It returns the file size.
func (t *tableWriter) finish() (int64, error) {
	if err := t.flushBlock(); err != nil {
		return 0, err
	}
	bloomOff, bloomLen, err := t.writeChecked(bloomBuild(t.hashes))
	if err != nil {
		return 0, err
	}
	var ib bytes.Buffer
	binary.Write(&ib, binary.LittleEndian, uint32(len(t.index)))
	for _, ie := range t.index {
		var tmp [binary.MaxVarintLen64]byte
		n := binary.PutUvarint(tmp[:], uint64(len(ie.key)))
		ib.Write(tmp[:n])
		ib.Write(ie.key)
		binary.Write(&ib, binary.LittleEndian, ie.seq)
		binary.Write(&ib, binary.LittleEndian, uint64(ie.off))
		binary.Write(&ib, binary.LittleEndian, ie.length)
	}
	idxOff, idxLen, err := t.writeChecked(ib.Bytes())
	if err != nil {
		return 0, err
	}
	var ft [footerSize]byte
	binary.LittleEndian.PutUint64(ft[0:], uint64(idxOff))
	binary.LittleEndian.PutUint64(ft[8:], uint64(idxLen))
	binary.LittleEndian.PutUint64(ft[16:], uint64(bloomOff))
	binary.LittleEndian.PutUint64(ft[24:], uint64(bloomLen))
	binary.LittleEndian.PutUint64(ft[32:], uint64(t.n))
	binary.LittleEndian.PutUint64(ft[40:], tableMagic)
	binary.LittleEndian.PutUint32(ft[48:], crc32.Checksum(ft[:48], castagnoli))
	if _, err := t.w.Write(ft[:]); err != nil {
		return 0, err
	}
	t.off += footerSize
	if err := t.w.Flush(); err != nil {
		return 0, err
	}
	if err := t.f.Sync(); err != nil {
		return 0, err
	}
	return t.off, t.f.Close()
}

func (t *tableWriter) abort() { t.f.Close() }

// table is an open, immutable SSTable. The index and bloom filter live in memory; blocks are read on demand.
type table struct {
	f     *os.File
	num   uint64
	size  int64
	n     int64
	index []indexEntry
	bloom []byte
}

func openTable(path string, num uint64) (*table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	fail := func(format string, a ...any) (*table, error) {
		f.Close()
		return nil, fmt.Errorf("%w: %s: %s", errCorrupt, path, fmt.Sprintf(format, a...))
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	if st.Size() < footerSize {
		return fail("file too short")
	}
	var ft [footerSize]byte
	if _, err := f.ReadAt(ft[:], st.Size()-footerSize); err != nil {
		f.Close()
		return nil, err
	}
	if binary.LittleEndian.Uint64(ft[40:]) != tableMagic || binary.LittleEndian.Uint32(ft[48:]) != crc32.Checksum(ft[:48], castagnoli) {
		return fail("bad footer")
	}
	idxOff, idxLen := int64(binary.LittleEndian.Uint64(ft[0:])), int64(binary.LittleEndian.Uint64(ft[8:]))
	bloomOff, bloomLen := int64(binary.LittleEndian.Uint64(ft[16:])), int64(binary.LittleEndian.Uint64(ft[24:]))
	t := &table{f: f, num: num, size: st.Size(), n: int64(binary.LittleEndian.Uint64(ft[32:]))}
	if idxLen < 4 || bloomLen < 4 || idxOff+idxLen > st.Size() || bloomOff+bloomLen > st.Size() {
		return fail("bad section bounds")
	}
	ib, err := t.readChecked(idxOff, idxLen)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: index: %w", path, err)
	}
	t.bloom, err = t.readChecked(bloomOff, bloomLen)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("%s: bloom: %w", path, err)
	}
	r := bytes.NewReader(ib)
	var cnt uint32
	if err := binary.Read(r, binary.LittleEndian, &cnt); err != nil {
		return fail("index count")
	}
	for i := uint32(0); i < cnt; i++ {
		kl, err := binary.ReadUvarint(r)
		if err != nil || kl > uint64(r.Len()) {
			return fail("index key")
		}
		key := make([]byte, kl)
		io.ReadFull(r, key)
		var seq, off uint64
		var l uint32
		if binary.Read(r, binary.LittleEndian, &seq) != nil || binary.Read(r, binary.LittleEndian, &off) != nil || binary.Read(r, binary.LittleEndian, &l) != nil {
			return fail("index entry")
		}
		t.index = append(t.index, indexEntry{key: key, seq: seq, off: int64(off), length: l})
	}
	return t, nil
}

// readChecked reads length bytes at off, verifies the trailing CRC and returns the payload without it.
func (t *table) readChecked(off, length int64) ([]byte, error) {
	if length < 4 || length > maxEntrySz {
		return nil, errCorrupt
	}
	b := make([]byte, length)
	if _, err := t.f.ReadAt(b, off); err != nil {
		return nil, err
	}
	payload := b[:length-4]
	if binary.LittleEndian.Uint32(b[length-4:]) != crc32.Checksum(payload, castagnoli) {
		return nil, fmt.Errorf("%w: checksum mismatch at %d", errCorrupt, off)
	}
	return payload, nil
}

func (t *table) block(i int) ([]entry, error) {
	b, err := t.readChecked(t.index[i].off, int64(t.index[i].length))
	if err != nil {
		return nil, err
	}
	var out []entry
	for p := 0; p < len(b); {
		kl, n := binary.Uvarint(b[p:])
		if n <= 0 || kl > uint64(len(b)-p-n) {
			return nil, errCorrupt
		}
		p += n
		key := b[p : p+int(kl)]
		p += int(kl)
		if len(b)-p < 10 {
			return nil, errCorrupt
		}
		seq := binary.LittleEndian.Uint64(b[p:])
		kind := Kind(b[p+8])
		p += 9
		vl, n := binary.Uvarint(b[p:])
		if n <= 0 || vl > uint64(len(b)-p-n) {
			return nil, errCorrupt
		}
		p += n
		out = append(out, entry{key: key, seq: seq, kind: kind, value: b[p : p+int(vl)]})
		p += int(vl)
	}
	return out, nil
}

// blockFor returns the last block whose first entry is at or before (key, seq).
func (t *table) blockFor(key []byte, seq uint64) int {
	i := sort.Search(len(t.index), func(i int) bool { return cmpInternal(t.index[i].key, t.index[i].seq, key, seq) > 0 }) - 1
	if i < 0 {
		i = 0
	}
	return i
}

// get returns the newest entry for key with seq <= maxSeq.
func (t *table) get(key []byte, maxSeq uint64) (entry, bool, error) {
	if len(t.index) == 0 || !bloomMay(t.bloom, key) {
		return entry{}, false, nil
	}
	for i := t.blockFor(key, maxSeq); i < len(t.index); i++ {
		es, err := t.block(i)
		if err != nil {
			return entry{}, false, err
		}
		for _, e := range es {
			c := bytes.Compare(e.key, key)
			if c < 0 {
				continue
			}
			if c > 0 {
				return entry{}, false, nil
			}
			if e.seq <= maxSeq {
				return e, true, nil
			}
		}
	}
	return entry{}, false, nil
}

type tableIter struct {
	t    *table
	bi   int
	es   []entry
	pos  int
	err  error
	done bool
}

func (t *table) iter() *tableIter {
	it := &tableIter{t: t, bi: -1}
	it.advanceBlock()
	return it
}

// iterFrom positions at the first entry whose user key is >= key.
func (t *table) iterFrom(key []byte) *tableIter {
	it := &tableIter{t: t}
	if len(t.index) == 0 {
		it.done = true
		return it
	}
	it.bi = t.blockFor(key, ^uint64(0)) - 1
	it.advanceBlock()
	for it.Valid() && bytes.Compare(it.es[it.pos].key, key) < 0 {
		it.Next()
	}
	return it
}

func (it *tableIter) advanceBlock() {
	for {
		it.bi++
		if it.bi >= len(it.t.index) {
			it.done = true
			return
		}
		es, err := it.t.block(it.bi)
		if err != nil {
			it.err, it.done = err, true
			return
		}
		if len(es) > 0 {
			it.es, it.pos = es, 0
			return
		}
	}
}

func (it *tableIter) Valid() bool  { return !it.done && it.err == nil }
func (it *tableIter) Entry() entry { return it.es[it.pos] }
func (it *tableIter) Err() error   { return it.err }
func (it *tableIter) Next() {
	it.pos++
	if it.pos >= len(it.es) {
		it.advanceBlock()
	}
}
