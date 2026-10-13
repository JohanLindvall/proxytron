package logtest

import (
	"context"
	"log/slog"
	"sync"
	"testing"
)

type Entry struct {
	Message string
	Stack   string
}
type Recorder struct {
	mu      sync.Mutex
	level   slog.Level
	entries []Entry
}

func Capture(t testing.TB, l slog.Level) *Recorder {
	r := &Recorder{level: l}
	old := slog.Default()
	slog.SetDefault(slog.New(r))
	t.Cleanup(func() { slog.SetDefault(old) })
	return r
}
func (r *Recorder) Enabled(_ context.Context, l slog.Level) bool { return l >= r.level }
func (r *Recorder) Handle(_ context.Context, v slog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := Entry{Message: v.Message}
	v.Attrs(func(a slog.Attr) bool {
		if a.Key == "stack" {
			entry.Stack = a.Value.String()
		}
		return true
	})
	r.entries = append(r.entries, entry)
	return nil
}
func (r *Recorder) WithAttrs([]slog.Attr) slog.Handler { return r }
func (r *Recorder) WithGroup(string) slog.Handler      { return r }
func (r *Recorder) Entries() []Entry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Entry(nil), r.entries...)
}
func (r *Recorder) Messages(string) []string {
	var out []string
	for _, v := range r.Entries() {
		out = append(out, v.Message)
	}
	return out
}

func (r *Recorder) Lines() []string { return r.Messages("") }
