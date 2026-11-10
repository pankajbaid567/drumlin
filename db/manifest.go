package db

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// The manifest lists the live SSTables per level. It is rewritten whole and swapped in atomically
// (temp file, fsync, rename, fsync of the directory), so after a crash it is either the old one or the new one.
type fileMeta struct {
	Num      uint64 `json:"num"`
	Size     int64  `json:"size"`
	Smallest []byte `json:"smallest"`
	Largest  []byte `json:"largest"`
	t        *table
}

type manifest struct {
	NextFile uint64        `json:"next_file"`
	LastSeq  uint64        `json:"last_seq"`
	LogNum   uint64        `json:"log_num"` // WAL files numbered below this are obsolete
	Levels   [][]*fileMeta `json:"levels"`
}

func manifestPath(dir string) string { return filepath.Join(dir, "MANIFEST") }

func loadManifest(dir string) (*manifest, error) {
	b, err := os.ReadFile(manifestPath(dir))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var m manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func writeManifest(dir string, m *manifest) error {
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, "MANIFEST.tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, manifestPath(dir)); err != nil {
		return err
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
