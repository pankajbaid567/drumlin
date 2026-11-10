package db

import (
	"bytes"
	"fmt"
	"strings"
)

// dumpKey lists every stored version of key, for diagnosing failures.
func (d *DB) dumpKey(key string) string {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var sb strings.Builder
	for name, m := range map[string]*skiplist{"mem": d.mem, "imm": d.imm} {
		if m == nil {
			continue
		}
		for it := m.iter(); it.Valid(); it.Next() {
			if string(it.Entry().key) == key {
				fmt.Fprintf(&sb, "%s: seq=%d kind=%d\n", name, it.Entry().seq, it.Entry().kind)
			}
		}
	}
	for l, fs := range d.levels {
		for _, f := range fs {
			for it := f.t.iter(); it.Valid(); it.Next() {
				if bytes.Equal(it.Entry().key, []byte(key)) {
					fmt.Fprintf(&sb, "L%d file %d [%s..%s]: seq=%d kind=%d\n", l, f.Num, f.Smallest, f.Largest, it.Entry().seq, it.Entry().kind)
				}
			}
		}
	}
	fmt.Fprintf(&sb, "snaps=%v seq=%d", d.snaps, d.seq)
	return sb.String()
}
