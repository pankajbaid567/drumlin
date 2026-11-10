package db

// mergeIter merges sorted iterators into one internal-key-ordered stream. With a handful of sources a linear
// scan for the minimum is as fast as a heap and simpler.
type mergeIter struct {
	its []iterator
	cur int
	err error
}

func newMerge(its []iterator) *mergeIter {
	m := &mergeIter{its: its, cur: -1}
	m.pick()
	return m
}

func (m *mergeIter) pick() {
	m.cur = -1
	for i, it := range m.its {
		if err := it.Err(); err != nil && m.err == nil {
			m.err = err
		}
		if !it.Valid() {
			continue
		}
		if m.cur < 0 || cmpEntry(it.Entry(), m.its[m.cur].Entry()) < 0 {
			m.cur = i
		}
	}
}

func (m *mergeIter) Valid() bool  { return m.err == nil && m.cur >= 0 }
func (m *mergeIter) Entry() entry { return m.its[m.cur].Entry() }
func (m *mergeIter) Err() error   { return m.err }
func (m *mergeIter) Next() {
	m.its[m.cur].Next()
	m.pick()
}
