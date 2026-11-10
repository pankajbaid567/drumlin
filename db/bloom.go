package db

import "hash/fnv"

// A Bloom filter over user keys: 10 bits per key, 7 probes (about 1% false positives). Layout: k byte | bit array.
const bloomK = 7

func hash32(key []byte) uint32 {
	h := fnv.New32a()
	h.Write(key)
	return h.Sum32()
}

func bloomBuild(hashes []uint32) []byte {
	bits := len(hashes) * 10
	if bits < 64 {
		bits = 64
	}
	nbytes := (bits + 7) / 8
	bits = nbytes * 8
	f := make([]byte, 1+nbytes)
	f[0] = bloomK
	for _, h := range hashes {
		delta := h>>17 | h<<15
		for i := 0; i < bloomK; i++ {
			b := h % uint32(bits)
			f[1+b/8] |= 1 << (b % 8)
			h += delta
		}
	}
	return f
}

func bloomMay(f []byte, key []byte) bool {
	if len(f) < 2 {
		return true
	}
	bits := uint32(len(f)-1) * 8
	h := hash32(key)
	delta := h>>17 | h<<15
	for i := 0; i < int(f[0]); i++ {
		b := h % bits
		if f[1+b/8]&(1<<(b%8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}
