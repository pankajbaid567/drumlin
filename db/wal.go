package db

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"os"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// WAL record: len u32 | crc32c u32 | payload; payload: seq u64 | kind u8 | klen uvarint | key | value.
// A record is complete only if its full length is present and the CRC matches.
type walWriter struct {
	f     *os.File
	w     *bufio.Writer
	sync  bool
	bytes int64
}

func createWAL(path string, sync bool) (*walWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &walWriter{f: f, w: bufio.NewWriterSize(f, 64<<10), sync: sync}, nil
}

func (w *walWriter) append(e entry) error {
	var kl [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(kl[:], uint64(len(e.key)))
	payload := make([]byte, 0, 9+n+len(e.key)+len(e.value))
	payload = binary.LittleEndian.AppendUint64(payload, e.seq)
	payload = append(payload, byte(e.kind))
	payload = append(payload, kl[:n]...)
	payload = append(payload, e.key...)
	payload = append(payload, e.value...)
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(hdr[4:], crc32.Checksum(payload, castagnoli))
	if _, err := w.w.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := w.w.Write(payload); err != nil {
		return err
	}
	// Hand the record to the OS before acknowledging: this alone survives a process crash. fsync adds power-loss safety.
	if err := w.w.Flush(); err != nil {
		return err
	}
	if w.sync {
		if err := w.f.Sync(); err != nil {
			return err
		}
	}
	w.bytes += int64(8 + len(payload))
	return nil
}

func (w *walWriter) close() error {
	if err := w.w.Flush(); err != nil {
		return err
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	return w.f.Close()
}

// readWAL returns every complete record, the byte length they cover, and whether trailing bytes were discarded (a torn tail).
func readWAL(path string) (entries []entry, valid int64, torn bool, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, false, err
	}
	pos := 0
	for {
		if len(data)-pos < 8 {
			break
		}
		n := int(binary.LittleEndian.Uint32(data[pos:]))
		crc := binary.LittleEndian.Uint32(data[pos+4:])
		if n < 10 || n > len(data)-pos-8 {
			break
		}
		payload := data[pos+8 : pos+8+n]
		if crc32.Checksum(payload, castagnoli) != crc {
			break
		}
		seq := binary.LittleEndian.Uint64(payload)
		kind := Kind(payload[8])
		kl, m := binary.Uvarint(payload[9:])
		if m <= 0 || kl > uint64(len(payload)-9-m) || kind > KindPut {
			break
		}
		ks := 9 + m
		key := append([]byte(nil), payload[ks:ks+int(kl)]...)
		val := append([]byte(nil), payload[ks+int(kl):]...)
		entries = append(entries, entry{key: key, seq: seq, kind: kind, value: val})
		pos += 8 + n
	}
	return entries, int64(pos), pos < len(data), nil
}
