package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// A long-lived store holds many free pages, and each saved outcome is a
// commit. Written on every commit, the list of them made each one allocate in
// proportion to it. A store written the old way still opens, saving
// allocates for what it writes, and the result opens under bbolt's defaults.
func TestASaveDoesNotRewriteTheFreePageList(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool.db")
	// Pages of 4 KB whatever the platform's, so the free pages come to the
	// same count everywhere: macOS's 16 KB pages left a third as many.
	old, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second, PageSize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	padding := bytes.Repeat([]byte{'x'}, 3000)
	for _, step := range []func(*bolt.Bucket, []byte) error{
		func(b *bolt.Bucket, k []byte) error { return b.Put(k, padding) },
		func(b *bolt.Bucket, k []byte) error { return b.Delete(k) },
		func(b *bolt.Bucket, k []byte) error { return nil }, // releases what was freed
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
			return b.Put([]byte("marker"), nil)
		}); err != nil {
			t.Fatal(err)
		}
	}
	if free := old.Stats().FreePageN; free < 10000 {
		t.Fatalf("only %d free pages to work with", free)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}

	s, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	row := Record{URL: "http://route.example.test:80", Data: json.RawMessage(`{"url":"http://route.example.test:80"}`)}
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	const saves = 50
	for range saves {
		if err := s.SaveProxy(t.Context(), row, nil); err != nil {
			t.Fatal(err)
		}
	}
	runtime.ReadMemStats(&after)
	if per := (after.TotalAlloc - before.TotalAlloc) / saves; per > 64<<10 {
		t.Errorf("a save allocated %d KB; want it to allocate for what it writes, not for every free page", per>>10)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	again, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := again.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	inv, err := s.LoadProxies(t.Context())
	if err != nil || len(inv.Rows) != 1 || string(inv.Rows[0]) != string(row.Data) {
		t.Fatalf("after reopening: %+v, %v", inv, err)
	}
}
