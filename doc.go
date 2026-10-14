// Package proxytron provides scored proxy rotation, discovery, persistence,
// proxy dialing and real egress IP detection.
//
// Request pools rank routes by expected successful responses per second and
// maintain independent service scopes, transport cooldowns and challenge
// cooldowns. Use DefaultConfig and LoadPool to restore a durable pool before
// discovery. FromURLs constructs an in-memory pool from already validated URLs.
// A direct route must be explicitly enabled by the application; an empty
// acquisition means no eligible route, never permission to bypass the pool.
// Pool.Do adds callback-driven retries with separate operation and route budgets.
// Pacer shares adaptive request spacing across routes on the same proxy host.
//
// The lease subpackage provides reservations spanning setup and long transfers,
// per-address concurrency limits and adaptive throughput selection. Neither
// package knows about application domains, page parsers or browser fingerprints.
// Applications report their classified outcomes and can supply HTTP adapters.
// lease.ExitProber probes and claims exits through a reserved session, with
// caller-selected probe services and an explicit policy for probe outages.
//
// Network activity is explicit: LoadPool can refresh configured feeds, Detector
// probes only its configured IP-check URL, and NewTransport performs no I/O.
// Tests use local servers and synthetic endpoints, without public proxy feeds.
package proxytron
