// SPDX-License-Identifier: MIT

package lease

import (
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// A long-lived database holds tens of thousands of free pages, and the pool
// commits once per route outcome. Written on every commit, that list made
// each one allocate in proportion to it: 756 KB a commit on a live database
// with 46,000 free pages. A database written the old way still opens, its
// commits allocate in proportion to what they write, and the result opens
// under bbolt's defaults again.
func TestACommitDoesNotRewriteTheFreePageList(t *testing.T) {
	path := filepath.Join(t.TempDir(), "proxies.db")
	old, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	padding := bytes.Repeat([]byte{'x'}, 3000)
	for _, step := range []func(*bolt.Bucket, []byte) error{
		func(b *bolt.Bucket, k []byte) error { return b.Put(k, padding) },
		func(b *bolt.Bucket, k []byte) error { return b.Delete(k) },
	} {
		if err := old.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("padding"))
			if err != nil {
				return err
			}
			for i := range 12000 {
				if err := step(b, fmt.Appendf(nil, "key-%05d", i)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	// Pages a commit frees are pending until a later commit releases them.
	if err := old.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("padding")).Put([]byte("released"), nil)
	}); err != nil {
		t.Fatal(err)
	}
	if free := old.Stats().FreePageN; free < 10000 {
		t.Fatalf("only %d free pages to work with", free)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}

	route := "http://route.example.test:80"
	p, err := Open(path, []string{route}, nil, NewHTTPClient(nil), slog.New(slog.NewTextHandler(io.Discard, nil)), testPolicy())
	if err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	e := p.entries[route]
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	const commits = 50
	for range commits {
		p.persist(e, "service")
	}
	runtime.ReadMemStats(&after)
	p.mu.Unlock()
	if per := (after.TotalAlloc - before.TotalAlloc) / commits; per > 64<<10 {
		t.Errorf("a commit allocated %d KB; want it to allocate for what it writes, not for every free page", per>>10)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}

	again, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer again.Close()
	if err := again.View(func(tx *bolt.Tx) error {
		if tx.Bucket(entriesBucket).Get([]byte(route)) == nil {
			return fmt.Errorf("the route was not kept")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
