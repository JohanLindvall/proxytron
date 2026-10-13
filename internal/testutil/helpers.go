package testutil

import (
	"flag"
	"os"
	"path/filepath"
	"testing"
)

var Update = flag.Bool("update", false, "update golden fixtures")

func Must[T any](v T, err error) func(testing.TB) T {
	return func(t testing.TB) T {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
}
func NoErr(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func ReadGolden(t testing.TB, path string) []byte { t.Helper(); return Must(os.ReadFile(path))(t) }
func WriteGolden(t testing.TB, path string, data []byte) {
	t.Helper()
	if !*Update {
		return
	}
	NoErr(t, os.MkdirAll(filepath.Dir(path), 0755))
	NoErr(t, os.WriteFile(path, data, 0644))
}
