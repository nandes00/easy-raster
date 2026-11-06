package tracker

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestSessionStoreUpdateAndLookup(t *testing.T) {
	store := NewSessionStore(time.Minute)
	addr := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 5023}

	store.Update("device-1", addr)

	got, ok := store.Lookup("device-1")
	if !ok {
		t.Fatalf("expected session to be found")
	}
	if got.String() != addr.String() {
		t.Fatalf("got addr %s, want %s", got, addr)
	}
}

func TestSessionStoreLookupMissing(t *testing.T) {
	store := NewSessionStore(time.Minute)

	if _, ok := store.Lookup("unknown"); ok {
		t.Fatalf("expected no session for unknown device")
	}
}

func TestSessionStoreExpiredSessionIsNotReturned(t *testing.T) {
	store := NewSessionStore(10 * time.Millisecond)
	addr := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 5023}

	store.Update("device-1", addr)
	time.Sleep(20 * time.Millisecond)

	if _, ok := store.Lookup("device-1"); ok {
		t.Fatalf("expected session to have expired")
	}
}

func TestSessionStoreUpdateRefreshesTTL(t *testing.T) {
	store := NewSessionStore(30 * time.Millisecond)
	addr := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 5023}

	store.Update("device-1", addr)
	time.Sleep(20 * time.Millisecond)
	store.Update("device-1", addr)
	time.Sleep(20 * time.Millisecond)

	if _, ok := store.Lookup("device-1"); !ok {
		t.Fatalf("expected session refreshed by second Update to still be valid")
	}
}

func TestSessionStoreSweepRemovesExpired(t *testing.T) {
	store := NewSessionStore(10 * time.Millisecond)
	addr := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 5023}

	store.Update("device-1", addr)
	time.Sleep(20 * time.Millisecond)
	store.Sweep()

	store.mu.RLock()
	_, exists := store.sessions["device-1"]
	store.mu.RUnlock()

	if exists {
		t.Fatalf("expected expired session to be removed by Sweep")
	}
}

func TestSessionStoreIgnoresEmptyIdentifierOrNilAddr(t *testing.T) {
	store := NewSessionStore(time.Minute)
	addr := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 5023}

	store.Update("", addr)
	store.Update("device-1", nil)

	if _, ok := store.Lookup(""); ok {
		t.Fatalf("expected empty identifier to not be stored")
	}
	if _, ok := store.Lookup("device-1"); ok {
		t.Fatalf("expected nil addr to not be stored")
	}
}

func TestSessionStoreRunStopsOnContextCancel(t *testing.T) {
	store := NewSessionStore(10 * time.Millisecond)
	addr := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 5023}
	store.Update("device-1", addr)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		store.Run(ctx, 5*time.Millisecond)
		close(done)
	}()

	time.Sleep(30 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("expected Run to return after context cancellation")
	}

	if _, ok := store.Lookup("device-1"); ok {
		t.Fatalf("expected session to have been swept before Run returned")
	}
}
