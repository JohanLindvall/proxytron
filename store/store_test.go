package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

func TestTransactionOrderRollbackAndOwnership(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pool.db")
	s, err := Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	rows := []Record{{URL: "b", Data: json.RawMessage(`{"url":"b","served":1}`)}, {URL: "a", Data: json.RawMessage(`{"url":"a"}`)}}
	first := json.RawMessage(`{"service": [1, 1, 2, 1]}`)
	sources := []Source{{URL: "https://feed.test", Scope: "region", Data: json.RawMessage(`{ "members": {} }`)}}
	if err := s.SaveProxies(t.Context(), rows, sources, first, map[string]float64{"retired": 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveProxy(t.Context(), Record{URL: "b", Data: json.RawMessage(`{"url":"b","served":2}`)}, first); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveProxies(t.Context(), []Record{rows[0], rows[0]}, nil, first, nil); err == nil {
		t.Fatal("duplicate keys accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.SaveProxies(ctx, nil, nil, first, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled transaction: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(t.Context(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	inv, err := s.LoadProxies(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(inv.Rows) != 2 || string(inv.Rows[0]) != `{"url":"b","served":2}` || string(inv.Rows[1]) != string(rows[1].Data) || !reflect.DeepEqual(inv.Sources, sources) || string(inv.First) != string(first) || !inv.Imported {
		t.Fatalf("bad snapshot: %+v", inv)
	}
	inv.Rows[0][0] = 'x'
	again, err := s.LoadProxies(t.Context())
	if err != nil || again.Rows[0][0] != '{' {
		t.Fatal("returned data aliases database memory")
	}
	if info, err := os.Stat(path); err != nil || (runtime.GOOS != "windows" && info.Mode().Perm() != 0600) {
		t.Fatalf("store permissions: %v %v", info, err)
	}
}
