// Package store defines the request pool's persistence boundary and provides a
// transactional Bolt implementation. Applications can implement Store to keep
// inventory in an existing database.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Record is an endpoint's complete scored state, encoded as JSON by the pool.
type Record struct {
	URL  string
	Data json.RawMessage
}

// Source is a discovery feed's last listing for an optional scope.
type Source struct {
	URL   string
	Scope string
	Data  json.RawMessage
}

// Inventory is an ordered snapshot. First contains learned selection priors.
// Imported records whether an opted-in legacy import has already run.
type Inventory struct {
	Rows     []json.RawMessage
	Sources  []Source
	First    json.RawMessage
	Retired  map[string]float64
	Imported bool
}

// Store persists complete outcomes atomically with their learned priors.
// SaveProxies replaces the inventory, preserving the supplied row order.
// Implementations must not retain mutable references to supplied data.
type Store interface {
	LoadProxies(context.Context) (Inventory, error)
	SaveProxy(context.Context, Record, json.RawMessage) error
	SaveProxies(context.Context, []Record, []Source, json.RawMessage, map[string]float64) error
	Path() string
	Close() error
}

// Bolt is a durable, single-process inventory store. Other processes cannot
// open the same file while it is in use. Opening a locked file fails after one
// second. Files are created with mode 0600 because endpoints may hold secrets.
type Bolt struct{ db *bolt.DB }

var bucket = []byte("proxytron-requests-v1")
var recordsKey = []byte("records")
var orderKey = []byte("order")
var metaKey = []byte("metadata")
var firstKey = []byte("first")

// Opaque documents stay byte-for-byte intact across the storage boundary.
type diskSource struct {
	URL, Scope string
	Data       []byte
}
type diskMetadata struct {
	Sources  []diskSource
	Retired  map[string]float64
	Imported bool
}

// Open opens a Bolt store without starting goroutines or contacting the network.
func Open(ctx context.Context, path string) (Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if path == "" {
		return nil, errors.New("proxytron: empty store path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("proxytron: open store: %w", err)
	}
	s := &Bolt{db: db}
	err = db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(bucket)
		if err != nil {
			return err
		}
		_, err = b.CreateBucketIfNotExists(recordsKey)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

// Path returns the path used to open the store.
func (s *Bolt) Path() string { return s.db.Path() }

// Close releases the file lock and closes the store.
func (s *Bolt) Close() error { return s.db.Close() }

func readOrder(b *bolt.Bucket) ([]string, error) {
	var order []string
	if data := b.Get(orderKey); data != nil {
		if err := json.Unmarshal(data, &order); err != nil {
			return nil, fmt.Errorf("proxytron: corrupt inventory order: %w", err)
		}
	}
	return order, nil
}

// LoadProxies reads an independent inventory snapshot, retaining exploration order.
func (s *Bolt) LoadProxies(ctx context.Context) (Inventory, error) {
	inv := Inventory{First: json.RawMessage(`{}`), Retired: map[string]float64{}}
	if err := ctx.Err(); err != nil {
		return inv, err
	}
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if data := b.Get(metaKey); data != nil {
			var meta diskMetadata
			if err := json.Unmarshal(data, &meta); err != nil {
				return fmt.Errorf("proxytron: corrupt metadata: %w", err)
			}
			inv.Retired, inv.Imported = meta.Retired, meta.Imported
			for _, src := range meta.Sources {
				inv.Sources = append(inv.Sources, Source{URL: src.URL, Scope: src.Scope, Data: src.Data})
			}
		}
		if first := b.Get(firstKey); first != nil {
			inv.First = append(json.RawMessage(nil), first...)
		}
		order, err := readOrder(b)
		if err != nil {
			return err
		}
		records := b.Bucket(recordsKey)
		for _, key := range order {
			if err := ctx.Err(); err != nil {
				return err
			}
			data := records.Get([]byte(key))
			if data == nil {
				return errors.New("proxytron: missing inventory record")
			}
			inv.Rows = append(inv.Rows, append(json.RawMessage(nil), data...))
		}
		return nil
	})
	return inv, err
}

func write(b *bolt.Bucket, key []byte, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return b.Put(key, data)
}

// SaveProxy commits one outcome and its priors in one transaction. Updating an
// existing route does not rewrite the rest of the inventory.
func (s *Bolt) SaveProxy(ctx context.Context, row Record, first json.RawMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		records := b.Bucket(recordsKey)
		if records.Get([]byte(row.URL)) == nil {
			order, err := readOrder(b)
			if err != nil {
				return err
			}
			if err = write(b, orderKey, append(order, row.URL)); err != nil {
				return err
			}
		}
		if err := records.Put([]byte(row.URL), row.Data); err != nil {
			return err
		}
		return b.Put(firstKey, first)
	})
}

// SaveProxies atomically replaces inventory, feeds, priors and retirement markers.
func (s *Bolt) SaveProxies(ctx context.Context, rows []Record, sources []Source, first json.RawMessage, retired map[string]float64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		if err := b.DeleteBucket(recordsKey); err != nil {
			return err
		}
		records, err := b.CreateBucket(recordsKey)
		if err != nil {
			return err
		}
		order := make([]string, 0, len(rows))
		seen := map[string]bool{}
		for _, row := range rows {
			if err := ctx.Err(); err != nil {
				return err
			}
			if row.URL == "" || seen[row.URL] {
				return errors.New("proxytron: empty or duplicate record key")
			}
			seen[row.URL] = true
			if err := records.Put([]byte(row.URL), row.Data); err != nil {
				return err
			}
			order = append(order, row.URL)
		}
		if err := write(b, orderKey, order); err != nil {
			return err
		}
		if err := b.Put(firstKey, first); err != nil {
			return err
		}
		meta := diskMetadata{Retired: retired, Imported: true}
		for _, src := range sources {
			meta.Sources = append(meta.Sources, diskSource{URL: src.URL, Scope: src.Scope, Data: src.Data})
		}
		return write(b, metaKey, meta)
	})
}
