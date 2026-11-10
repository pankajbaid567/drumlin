// Command bench is a db_bench-style benchmark: fill sequential, fill random, read random, scan.
// Closed loop, single client, one machine. Write amplification comes from the store's own byte counters.
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"time"

	"drumlin/db"
)

func main() {
	n := flag.Int("n", 1_000_000, "keys")
	valSize := flag.Int("value", 100, "value bytes")
	sync := flag.Bool("sync", false, "fsync every write")
	dir := flag.String("dir", "", "directory (default temp)")
	flag.Parse()
	if *dir == "" {
		*dir, _ = os.MkdirTemp("", "drumlin-bench")
	}
	val := make([]byte, *valSize)
	for i := range val {
		val[i] = byte('a' + i%26)
	}
	key := func(i int) []byte { return []byte(fmt.Sprintf("key%012d", i)) }

	run := func(name string, o db.Options, work func(d *db.DB, lat *[]time.Duration)) {
		path := *dir + "/" + name
		os.RemoveAll(path)
		d, err := db.Open(path, o)
		if err != nil {
			panic(err)
		}
		var lat []time.Duration
		t0 := time.Now()
		work(d, &lat)
		el := time.Since(t0)
		s := d.Stats()
		d.Close()
		sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
		p := func(q float64) time.Duration { return lat[int(float64(len(lat)-1)*q)] }
		fmt.Printf("%-22s %9.0f ops/s  p50 %-9v p99 %-9v p99.9 %-9v max %-9v  write-amp %.2f (user %.0f MB, disk %.0f MB, %d compactions)\n",
			name, float64(len(lat))/el.Seconds(), p(0.5), p(0.99), p(0.999), lat[len(lat)-1], s.WriteAmplification(),
			float64(s.UserBytes)/1e6, float64(s.WALBytes+s.FlushBytes+s.CompactionBytes)/1e6, s.Compactions)
	}
	opts := db.Options{Sync: *sync}
	fill := func(order func(i int) int) func(d *db.DB, lat *[]time.Duration) {
		return func(d *db.DB, lat *[]time.Duration) {
			for i := 0; i < *n; i++ {
				t := time.Now()
				if err := d.Put(key(order(i)), val); err != nil {
					panic(err)
				}
				*lat = append(*lat, time.Since(t))
			}
		}
	}
	run("fillseq", opts, fill(func(i int) int { return i }))
	r := rand.New(rand.NewSource(1))
	perm := r.Perm(*n)
	run("fillrandom", opts, fill(func(i int) int { return perm[i] }))

	// read workloads run on the fillrandom database
	path := *dir + "/fillrandom"
	d, err := db.Open(path, opts)
	if err != nil {
		panic(err)
	}
	var lat []time.Duration
	reads := *n
	if reads > 500_000 {
		reads = 500_000
	}
	t0 := time.Now()
	miss := 0
	for i := 0; i < reads; i++ {
		t := time.Now()
		_, ok, err := d.Get(key(r.Intn(*n)))
		if err != nil {
			panic(err)
		}
		if !ok {
			miss++
		}
		lat = append(lat, time.Since(t))
	}
	el := time.Since(t0)
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	fmt.Printf("%-22s %9.0f ops/s  p50 %-9v p99 %-9v p99.9 %-9v max %-9v  (misses: %d)\n", "readrandom", float64(reads)/el.Seconds(),
		lat[len(lat)/2], lat[int(float64(len(lat)-1)*0.99)], lat[int(float64(len(lat)-1)*0.999)], lat[len(lat)-1], miss)
	t0 = time.Now()
	total := 0
	for i := 0; i < 200; i++ {
		kvs, err := d.Scan(key(r.Intn(*n)), nil, 100)
		if err != nil {
			panic(err)
		}
		total += len(kvs)
	}
	el = time.Since(t0)
	fmt.Printf("%-22s %9.0f scans/s of 100 keys (%d keys returned)\n", "scan100", 200/el.Seconds(), total)
	s := d.Stats()
	fmt.Printf("levels after fillrandom (files per level): %v\n", s.LevelFiles)
	d.Close()
}
