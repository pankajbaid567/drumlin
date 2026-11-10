// Command crashtest kills a writer process with SIGKILL at random moments and checks that recovery loses nothing
// that was acknowledged and invents nothing.
//
// The writer applies a deterministic sequence of operations (puts, overwrites, deletes), one at a time, and prints
// the number of each operation after the store acknowledged it. The checker knows the sequence, so after a kill the
// store must equal the state after operation N, where N is the last acknowledged operation or the one after it (the
// operation that was in flight when the process died may or may not have reached the log). Anything else is a
// violation. Recovery is checked on a COPY of the crashed directory, so the next writer starts from the real crashed
// state, including kills that land in the middle of recovery, flushes and compactions.
//
// Only the process is killed. The operating system and the disk are not, so this shows crash consistency of the
// process, not power-loss safety.
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"drumlin/db"
)

const keySpace = 3000

func opKey(i int) []byte    { return []byte(fmt.Sprintf("k%05d", (i*2654435761)%keySpace)) }
func opIsDelete(i int) bool { return i%9 == 0 }
func opValue(i int) []byte {
	n := 100 + (i*37)%1400
	return append([]byte(fmt.Sprintf("v%d:", i)), bytes.Repeat([]byte{byte('a' + i%26)}, n)...)
}

func options(sync bool) db.Options {
	// tiny tables so that flushes and compactions are constantly in progress when the kill lands
	return db.Options{MemtableBytes: 128 << 10, L0Trigger: 3, BaseLevelBytes: 512 << 10, FileBytes: 128 << 10, Sync: sync}
}

func main() {
	child := flag.Bool("child", false, "internal: run as the writer")
	dir := flag.String("dir", "", "database directory (default: a temp dir)")
	start := flag.Int("start", 1, "internal: first operation number")
	runs := flag.Int("runs", 1000, "number of kills")
	seed := flag.Int64("seed", 1, "random seed for kill timing")
	minMs := flag.Int("min-ms", 15, "earliest kill after launch")
	maxMs := flag.Int("max-ms", 400, "latest kill after launch")
	sync := flag.Bool("sync", false, "fsync the log on every write")
	csvPath := flag.String("csv", "", "write one row per run to this file")
	flag.Parse()
	if *child {
		runChild(*dir, *start, *sync)
		return
	}
	os.Exit(runParent(*dir, *runs, *seed, *minMs, *maxMs, *sync, *csvPath))
}

func runChild(dir string, start int, sync bool) {
	d, err := db.Open(dir, options(sync))
	if err != nil {
		fmt.Fprintln(os.Stderr, "child open:", err)
		os.Exit(3)
	}
	for i := start; ; i++ {
		var err error
		if opIsDelete(i) {
			err = d.Delete(opKey(i))
		} else {
			err = d.Put(opKey(i), opValue(i))
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "child write:", err)
			os.Exit(3)
		}
		os.Stdout.WriteString(strconv.Itoa(i) + "\n") // acknowledged
	}
}

func runParent(dir string, runs int, seed int64, minMs, maxMs int, sync bool, csvPath string) int {
	self, _ := os.Executable()
	if dir == "" {
		dir, _ = os.MkdirTemp("", "drumlin-crash")
	}
	scratch, _ := os.MkdirTemp("", "drumlin-verify")
	rnd := rand.New(rand.NewSource(seed))
	model := map[string]string{}
	applied := 0 // model reflects operations 1..applied
	apply := func(to int) {
		for ; applied < to; applied++ {
			i := applied + 1
			if opIsDelete(i) {
				delete(model, string(opKey(i)))
			} else {
				model[string(opKey(i))] = string(opValue(i))
			}
		}
	}
	var csv *os.File
	if csvPath != "" {
		csv, _ = os.Create(csvPath)
		fmt.Fprintln(csv, "run,kill_after_ms,acked_in_run,persisted_ops,matched,sst_files")
	}
	totalAcked, inFlightSurvived, killedInOpen := 0, 0, 0
	t0 := time.Now()
	for run := 1; run <= runs; run++ {
		cmd := exec.Command(self, "-child", "-dir", dir, "-start", strconv.Itoa(applied+1), fmt.Sprintf("-sync=%v", sync))
		out, _ := cmd.StdoutPipe()
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			fmt.Println("cannot start child:", err)
			return 2
		}
		runStart := applied
		lastAck := applied
		got := make(chan int, 1)
		go func() {
			last := applied
			sc := bufio.NewScanner(out)
			for sc.Scan() {
				if n, err := strconv.Atoi(sc.Text()); err == nil {
					last = n
				}
			}
			got <- last
		}()
		wait := time.Duration(minMs+rnd.Intn(maxMs-minMs+1)) * time.Millisecond
		time.Sleep(wait)
		cmd.Process.Signal(syscall.SIGKILL)
		cmd.Wait()
		lastAck = <-got
		if lastAck == applied {
			killedInOpen++
		}

		// verify on a copy so the real directory stays exactly as the crash left it
		verifyDir := filepath.Join(scratch, "v")
		os.RemoveAll(verifyDir)
		if err := copyDir(dir, verifyDir); err != nil {
			fmt.Println("copy:", err)
			return 2
		}
		d, err := db.Open(verifyDir, options(false))
		if err != nil {
			fmt.Printf("VIOLATION run %d: store does not reopen after a kill: %v\n", run, err)
			return 1
		}
		apply(lastAck)
		diff := compare(d, model)
		matched := lastAck
		if diff != "" {
			apply(lastAck + 1) // the in-flight operation may have been durable
			if d2 := compare(d, model); d2 != "" {
				fmt.Printf("VIOLATION run %d (last acknowledged op %d, killed after %v): %s\n  and also after op %d: %s\n", run, lastAck, wait, diff, lastAck+1, d2)
				d.Close()
				return 1
			}
			matched = lastAck + 1
			inFlightSurvived++
		}
		ssts, _ := filepath.Glob(filepath.Join(verifyDir, "*.sst"))
		d.Close()
		totalAcked += lastAck - runStart
		if csv != nil {
			fmt.Fprintf(csv, "%d,%d,%d,%d,%d,%d\n", run, wait.Milliseconds(), lastAck-runStart, applied, matched, len(ssts))
		}
		if run%100 == 0 {
			fmt.Printf("%d runs ok, %d operations so far\n", run, applied)
		}
	}
	os.RemoveAll(scratch)
	fmt.Printf("PASS: %d kills, %d acknowledged operations checked, %d runs where the in-flight operation had also been logged, %d kills before the first write, %d seconds\n",
		runs, totalAcked, inFlightSurvived, killedInOpen, int(time.Since(t0).Seconds()))
	return 0
}

func compare(d *db.DB, model map[string]string) string {
	kvs, err := d.Scan(nil, nil, 0)
	if err != nil {
		return "scan failed: " + err.Error()
	}
	got := map[string]string{}
	for _, kv := range kvs {
		got[string(kv.Key)] = string(kv.Value)
	}
	var problems []string
	for k, v := range model {
		if gv, ok := got[k]; !ok {
			problems = append(problems, "missing "+k)
		} else if gv != v {
			problems = append(problems, "wrong value for "+k)
		}
	}
	for k := range got {
		if _, ok := model[k]; !ok {
			problems = append(problems, "unexpected "+k)
		}
	}
	// spot-check point reads agree with the scan
	for k, v := range model {
		if gv, ok, err := d.Get([]byte(k)); err != nil || !ok || string(gv) != v {
			problems = append(problems, "get disagrees for "+k)
			break
		}
	}
	sort.Strings(problems)
	if len(problems) > 5 {
		problems = append(problems[:5], fmt.Sprintf("... %d problems", len(problems)))
	}
	return strings.Join(problems, "; ")
}

func copyDir(src, dst string) error {
	if err := os.MkdirAll(dst, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	for _, e := range entries {
		in, err := os.Open(filepath.Join(src, e.Name()))
		if err != nil {
			if os.IsNotExist(err) {
				continue // a compaction removed it between the listing and now: the writer is dead, so this cannot happen
			}
			return err
		}
		out, err := os.Create(filepath.Join(dst, e.Name()))
		if err != nil {
			in.Close()
			return err
		}
		_, err = io.Copy(out, in)
		in.Close()
		out.Close()
		if err != nil {
			return err
		}
	}
	return nil
}
