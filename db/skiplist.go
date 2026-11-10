package db

import "math/rand"

const maxHeight = 12

type node struct {
	e    entry
	next []*node
}

// skiplist is the memtable. Not safe for concurrent use: the DB serialises access.
type skiplist struct {
	head *node
	h    int
	rnd  *rand.Rand
	size int64
	n    int
}

func newSkiplist() *skiplist {
	return &skiplist{head: &node{next: make([]*node, maxHeight)}, h: 1, rnd: rand.New(rand.NewSource(1))}
}

func (s *skiplist) insert(e entry) {
	var prev [maxHeight]*node
	x := s.head
	for l := s.h - 1; l >= 0; l-- {
		for x.next[l] != nil && cmpEntry(x.next[l].e, e) < 0 {
			x = x.next[l]
		}
		prev[l] = x
	}
	height := 1
	for height < maxHeight && s.rnd.Intn(4) == 0 {
		height++
	}
	if height > s.h {
		for l := s.h; l < height; l++ {
			prev[l] = s.head
		}
		s.h = height
	}
	n := &node{e: e, next: make([]*node, height)}
	for l := 0; l < height; l++ {
		n.next[l] = prev[l].next[l]
		prev[l].next[l] = n
	}
	s.size += int64(len(e.key) + len(e.value) + 48)
	s.n++
}

// seek returns the first node at or after (key, seq) in internal order.
func (s *skiplist) seek(key []byte, seq uint64) *node {
	x := s.head
	for l := s.h - 1; l >= 0; l-- {
		for x.next[l] != nil && cmpInternal(x.next[l].e.key, x.next[l].e.seq, key, seq) < 0 {
			x = x.next[l]
		}
	}
	return x.next[0]
}

type memIter struct{ n *node }

func (s *skiplist) iter() *memIter               { return &memIter{n: s.head.next[0]} }
func (s *skiplist) iterFrom(key []byte) *memIter { return &memIter{n: s.seek(key, ^uint64(0))} }
func (m *memIter) Valid() bool                   { return m.n != nil }
func (m *memIter) Entry() entry                  { return m.n.e }
func (m *memIter) Next()                         { m.n = m.n.next[0] }
func (m *memIter) Err() error                    { return nil }
