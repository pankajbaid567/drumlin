package db

import "bytes"

// Kind is the type of a log entry.
type Kind byte

const (
	KindDel Kind = 0
	KindPut Kind = 1
)

// entry is one versioned key. Entries are ordered by user key ascending, then sequence number descending,
// so the newest version of a key comes first.
type entry struct {
	key   []byte
	seq   uint64
	kind  Kind
	value []byte
}

func cmpInternal(ak []byte, aseq uint64, bk []byte, bseq uint64) int {
	if c := bytes.Compare(ak, bk); c != 0 {
		return c
	}
	switch {
	case aseq > bseq:
		return -1
	case aseq < bseq:
		return 1
	}
	return 0
}

func cmpEntry(a, b entry) int { return cmpInternal(a.key, a.seq, b.key, b.seq) }

// iterator walks entries in internal-key order.
type iterator interface {
	Valid() bool
	Entry() entry
	Next()
	Err() error
}

// KV is a key and value returned by Scan.
type KV struct {
	Key, Value []byte
}
