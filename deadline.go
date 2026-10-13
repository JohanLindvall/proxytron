package proxytron

import (
	"net"
	"time"
)

// DeadlineConn gives each Read and each Write on Conn a deadline of its own,
// Timeout from the moment the call starts. It bounds every operation rather
// than the exchange, as a socket timeout does: a peer that keeps sending is
// never cut off, and one that falls silent for Timeout is. Wrapped around a
// *tls.Conn, a Read or Write is one TLS operation, so its deadline covers
// every socket read the record takes, and a peer dripping a record a byte at
// a time still times out. A zero or negative Timeout sets no deadline.
//
// The proxy feed downloads use it, under net/http. This package's own
// connections change their timeout as a request moves from connecting to
// reading and probe idle connections, and keep a wrapper of their own for
// that.
type DeadlineConn struct {
	net.Conn
	Timeout time.Duration
}

// Read reads from Conn under a read deadline Timeout from now.
func (c DeadlineConn) Read(p []byte) (int, error) {
	if err := c.SetReadDeadline(c.deadline()); err != nil {
		return 0, err
	}
	return c.Conn.Read(p)
}

// Write writes to Conn under a write deadline Timeout from now.
func (c DeadlineConn) Write(p []byte) (int, error) {
	if err := c.SetWriteDeadline(c.deadline()); err != nil {
		return 0, err
	}
	return c.Conn.Write(p)
}

// deadline is the deadline of an operation starting now; the zero time is
// none.
func (c DeadlineConn) deadline() time.Time {
	if c.Timeout <= 0 {
		return time.Time{}
	}
	return time.Now().Add(c.Timeout)
}
