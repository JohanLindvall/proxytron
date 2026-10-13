// SPDX-License-Identifier: MIT

package lease

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"time"

	"github.com/JohanLindvall/proxytron"
)

func normalize(raw string) (string, error) { return proxytron.CanonicalEndpoint(raw, "http") }

func parseFeed(body []byte) ([]string, error) {
	rows, err := proxytron.ParseFeed(body, "http", "")
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []string
	for _, row := range rows {
		u, err := normalize(row.URL)
		if err == nil && !seen[u] {
			seen[u] = true
			out = append(out, u)
		}
		if len(out) >= defaultMaxEntries {
			break
		}
	}
	return out, nil
}

func (p *Pool) fetch(ctx context.Context, src string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultFeedTimeout)
	defer cancel()
	req, err := p.client.NewRequest(ctx, http.MethodGet, src, nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.client.DoOnce(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("proxy feed: %s", resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, defaultFeedBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > defaultFeedBytes {
		return nil, errors.New("proxy feed is too large")
	}
	rows, err := parseFeed(body)
	if err != nil || p.feedFilter == nil {
		return rows, err
	}
	filtered := slices.DeleteFunc(rows, func(endpoint string) bool { return !p.feedFilter(endpoint) })
	if len(rows) > 0 && len(filtered) == 0 {
		return nil, errors.New("proxy feed contains no supported endpoints")
	}
	return filtered, nil
}

// Refresh retains surviving health and each failed source's last good list.
// Routine attempts are a day apart, including across restarts or failures.
// Earlier top-ups require runnable demand and too few usable, scored routes,
// and are at least five minutes apart. Network I/O holds neither the pool
// lock nor a Bolt transaction.
func (p *Pool) Refresh(ctx context.Context) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	if p.configDirty {
		if err := p.saveAll(); err != nil {
			p.log.Error("could not save proxy inventory", "err", err)
		} else {
			p.configDirty = false
			clear(p.dirty)
		}
	} else if err := p.flushMeasurements(); err != nil {
		p.log.Error("could not save proxy measurements", "err", err)
	}
	feeds := slices.Clone(p.feeds)
	p.mu.Unlock()
	for _, src := range feeds {
		if ctx.Err() != nil {
			return
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return
		}
		if !p.discovery || !slices.Contains(p.feeds, src) {
			p.mu.Unlock()
			continue
		}
		now := time.Now()
		s := p.sources[src]
		if s == nil {
			s = &source{}
			p.sources[src] = s
		}
		elapsed := now.Sub(s.Attempted)
		due := s.Attempted.IsZero() || elapsed >= defaultRefresh ||
			(elapsed >= defaultRefreshRetry && p.shortOfRoutes(now))
		if !due || s.refreshing {
			p.mu.Unlock()
			continue
		}
		s.Attempted, s.refreshing = now, true
		p.mu.Unlock()
		members, err := p.fetch(ctx, src)
		p.mu.Lock()
		s.refreshing = false
		if p.closed {
			p.mu.Unlock()
			return
		}
		if !p.discovery || !slices.Contains(p.feeds, src) {
			p.mu.Unlock()
			continue
		}
		s.Failed = err != nil && ctx.Err() == nil
		if err == nil {
			s.Fetched, s.Members = time.Now(), members
			for _, raw := range members {
				e := p.ensure(raw, s.Fetched)
				e.LastSeen = s.Fetched
				if !slices.Contains(e.Origins, src) {
					e.Origins = append(e.Origins, src)
				}
			}
		} else if ctx.Err() == nil {
			u, _ := url.Parse(src)
			p.log.Warn("proxy feed unavailable; keeping saved inventory", "feed", u.Redacted(), "err", err)
		}
		p.retire(time.Now())
		if err := p.saveAll(); err != nil {
			p.log.Error("could not save proxy inventory", "err", err)
		}
		p.mu.Unlock()
	}
}

// shortOfRoutes judges capacity, not the size of a feed. Untried entries and
// weak scores cannot conceal a depleted pool. A standby route must have
// succeeded and score at least as well as the learned prior for an untried
// route; this uses the UI's mean score, without drawing random samples.
// Active leases count during their first measurement window; after that,
// their measured score must qualify too. Ports and protocols on one address
// count only once, just as in Acquire.
// Caller holds mu.
func (p *Pool) shortOfRoutes(now time.Time) bool {
	for site, wanted := range p.demand {
		if wanted <= 0 {
			continue
		}
		prior := p.scorePrior(site)
		threshold := prior.meanRate(siteStat{})
		cooling, ready := make(map[string]bool), make(map[string]bool)
		for _, e := range p.entries {
			if e.readStat(site).Until.After(now) || (p.policy.isRecovery(site) && e.WAF.Until.After(now)) {
				cooling[e.host()] = true
			}
		}
		for _, e := range p.entries {
			key, s := e.host(), e.scoreStat(site)
			if e.Until.After(now) || cooling[key] {
				continue
			}
			starting := s.active > 0 && s.BytesPerSecond == 0
			scored := s.OK > 0 && prior.entryRate(e, site, p.policy.isRecovery(site), 0) >= threshold
			if starting || ((p.enabled(e) || s.active > 0) && scored) {
				ready[key] = true
				if len(ready) >= wanted {
					break
				}
			}
		}
		if len(ready) < wanted {
			return true
		}
	}
	return false
}

func (p *Pool) retire(now time.Time) {
	listed := make(map[string]bool)
	for _, src := range p.feeds {
		if s := p.sources[src]; s != nil {
			for _, raw := range s.Members {
				listed[raw] = true
			}
		}
	}
	for raw, e := range p.entries {
		if raw == Direct || listed[raw] || slices.Contains(p.static, raw) {
			continue
		}
		last := e.LastSuccess
		if last.IsZero() {
			last = e.FirstSeen
		}
		active := false
		for _, stat := range e.Sites {
			if stat.active > 0 {
				active = true
				break
			}
		}
		if !active && now.Sub(last) >= defaultRetireAfter {
			if e.client != nil {
				e.client.CloseIdleConnections()
			}
			delete(p.entries, raw)
			p.idx.valid = false
		}
	}
}
