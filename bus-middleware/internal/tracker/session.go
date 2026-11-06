package tracker

import (
	"context"
	"net"
	"sync"
	"time"
)

const defaultSessionTTL = 10 * time.Minute

type session struct {
	addr     *net.UDPAddr
	lastSeen time.Time
}

// SessionStore keeps track of the last known UDP address for each device, so
// replies (ACK/command) coming back from the forwarder can be routed to the
// right device without a fresh packet from it.
type SessionStore struct {
	mu       sync.RWMutex
	sessions map[string]session
	ttl      time.Duration
}

func NewSessionStore(ttl time.Duration) *SessionStore {
	if ttl <= 0 {
		ttl = defaultSessionTTL
	}
	return &SessionStore{
		sessions: make(map[string]session),
		ttl:      ttl,
	}
}

// Update records addr as the last known address for deviceIdentifier.
func (s *SessionStore) Update(deviceIdentifier string, addr *net.UDPAddr) {
	if deviceIdentifier == "" || addr == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[deviceIdentifier] = session{addr: addr, lastSeen: time.Now()}
}

// Lookup returns the last known address for deviceIdentifier, if the session
// hasn't expired.
func (s *SessionStore) Lookup(deviceIdentifier string) (*net.UDPAddr, bool) {
	s.mu.RLock()
	sess, ok := s.sessions[deviceIdentifier]
	s.mu.RUnlock()

	if !ok {
		return nil, false
	}

	if time.Since(sess.lastSeen) > s.ttl {
		s.mu.Lock()
		delete(s.sessions, deviceIdentifier)
		s.mu.Unlock()
		return nil, false
	}

	return sess.addr, true
}

// Sweep removes expired sessions so the table doesn't grow unbounded with
// devices that stopped reporting.
func (s *SessionStore) Sweep() {
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()
	for id, sess := range s.sessions {
		if now.Sub(sess.lastSeen) > s.ttl {
			delete(s.sessions, id)
		}
	}
}

// Run periodically sweeps expired sessions until ctx is cancelled.
func (s *SessionStore) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.Sweep()
		}
	}
}
