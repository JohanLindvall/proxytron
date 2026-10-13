package proxytron

import (
	"context"
	"errors"
	"log/slog"
	"net/url"
	"strconv"
	"time"

	"github.com/JohanLindvall/proxytron/store"
)

// Selection policies for request pools.
const (
	SelectionThroughput = "throughput"
	SelectionRoundRobin = "round_robin"
)

// Config configures static routes and an optional discovery feed. Scope names
// are caller-defined. A {country} feed template expands scopes in uppercase.
// Start with DefaultConfig and change the fields your application needs.
// Static endpoints retain their spelling for compatibility with existing stored
// keys. Call ParseEndpoint at the application's configuration boundary to validate
// new routes. Discovery feeds and the provided transports always validate routes.
type Config struct {
	// Revision lets caller-specific configuration changes invalidate a live override.
	Revision        string
	Endpoints       []string
	FeedURL         string
	DefaultScheme   string
	Scopes          []string
	RefreshInterval time.Duration
	MinReady        int
	Settings
}

// DefaultConfig returns conservative discovery and health defaults.
func DefaultConfig() Config {
	return Config{DefaultScheme: "socks5h", RefreshInterval: 24 * time.Hour, MinReady: 5,
		Settings: Settings{BaseCooldown: time.Minute, ChallengeCooldown: time.Hour,
			BrokenCooldown: 10 * time.Minute, DeadCooldown: 24 * time.Hour, Selection: SelectionThroughput}}
}

// Enabled reports whether static routes or discovery are configured.
func (c Config) Enabled() bool { return len(c.Endpoints) > 0 || c.FeedURL != "" }

// Validate checks scheduling, selection and cooldown settings.
func (c Config) Validate() error {
	if c.MinReady < 0 || c.RefreshInterval < 0 || c.BaseCooldown < 0 || c.ChallengeCooldown < 0 || c.BrokenCooldown < 0 || c.DeadCooldown < 0 {
		return errors.New("proxytron: negative configuration value")
	}
	if c.Selection != SelectionThroughput && c.Selection != SelectionRoundRobin {
		return errors.New(`proxytron: selection must be "throughput" or "round_robin"`)
	}
	if !proxyScheme(c.DefaultScheme) {
		return errors.New("proxytron: unsupported default scheme")
	}
	if c.FeedURL != "" {
		u, err := url.Parse(c.FeedURL)
		if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return errors.New("proxytron: feed must be an HTTP(S) URL")
		}
	}
	return nil
}

// StoreOpener opens an owned persistence store. The pool closes it on Close.
type StoreOpener func(context.Context, string) (store.Store, error)

// WithStoreOpener replaces the default Bolt store, for example with an
// adapter to an application's existing persistent database.
func WithStoreOpener(open StoreOpener) Option {
	return func(p *Pool) {
		if open != nil {
			p.openStore = open
		}
	}
}

// WithLogger supplies the logger for outcome and persistence diagnostics.
func WithLogger(log *slog.Logger) Option {
	return func(p *Pool) {
		if log != nil {
			p.log = log
		}
	}
}

// WithScopeContext allows an application to retain its contextual log metadata.
// The function must not call back into the pool.
func WithScopeContext(f func(context.Context, string) context.Context) Option {
	return func(p *Pool) {
		if f != nil {
			p.scopeContext = f
		}
	}
}

// WithLegacyImport enables importing proxy-memory.json beside the store once.
// Enable only when migrating an application that owns that file.
func WithLegacyImport(enabled bool) Option { return func(p *Pool) { p.legacyImport = enabled } }

func roundDecimal(x float64, places int) float64 {
	v, _ := strconv.ParseFloat(strconv.FormatFloat(x, 'f', places, 64), 64)
	return v
}
