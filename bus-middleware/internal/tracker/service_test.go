package tracker

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

type noopLogger struct{}

func (noopLogger) Info(msg string, args ...any)  {}
func (noopLogger) Error(msg string, args ...any) {}
func (noopLogger) Debug(msg string, args ...any) {}

type fakeDeviceWriter struct {
	writeErr error
	gotAddr  *net.UDPAddr
	gotData  []byte
}

func (f *fakeDeviceWriter) WriteToDevice(addr *net.UDPAddr, payload []byte) error {
	f.gotAddr = addr
	f.gotData = payload
	return f.writeErr
}

func TestServiceHandleUpdatesSessionFromDevicePacket(t *testing.T) {
	sessions := NewSessionStore(time.Minute)
	svc := NewService(nil, nil, sessions, "", noopLogger{})

	remoteAddr := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 54321}
	payload := []byte("STT;DEVICE123;rest;of;payload")

	svc.Handle(context.Background(), remoteAddr, payload)

	got, ok := sessions.Lookup("DEVICE123")
	if !ok {
		t.Fatalf("expected session to be populated for DEVICE123")
	}
	if got.String() != remoteAddr.String() {
		t.Fatalf("got addr %s, want %s", got, remoteAddr)
	}
}

func TestServiceHandleIgnoresPacketWithoutDeviceIdentifier(t *testing.T) {
	sessions := NewSessionStore(time.Minute)
	svc := NewService(nil, nil, sessions, "", noopLogger{})

	remoteAddr := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 54321}
	payload := []byte("garbage payload with no known protocol marker")

	svc.Handle(context.Background(), remoteAddr, payload)

	if _, ok := sessions.Lookup(""); ok {
		t.Fatalf("expected no session to be stored for an empty device identifier")
	}
}

func TestServiceHandleWorksWithoutSessionStore(t *testing.T) {
	svc := NewService(nil, nil, nil, "", noopLogger{})

	remoteAddr := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 54321}
	payload := []byte("STT;DEVICE123;rest;of;payload")

	svc.Handle(context.Background(), remoteAddr, payload) // must not panic
}

func TestServiceSendToDeviceWritesToKnownSession(t *testing.T) {
	sessions := NewSessionStore(time.Minute)
	writer := &fakeDeviceWriter{}
	svc := NewService(nil, nil, sessions, "", noopLogger{})
	svc.SetDeviceWriter(writer)

	addr := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 54321}
	sessions.Update("DEVICE123", addr)

	payload := []byte("ack-payload")
	if err := svc.SendToDevice("DEVICE123", payload); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if writer.gotAddr.String() != addr.String() {
		t.Fatalf("got addr %s, want %s", writer.gotAddr, addr)
	}
	if string(writer.gotData) != string(payload) {
		t.Fatalf("got payload %q, want %q", writer.gotData, payload)
	}
}

func TestServiceSendToDeviceReturnsErrNoSessionWhenUnknown(t *testing.T) {
	sessions := NewSessionStore(time.Minute)
	writer := &fakeDeviceWriter{}
	svc := NewService(nil, nil, sessions, "", noopLogger{})
	svc.SetDeviceWriter(writer)

	err := svc.SendToDevice("UNKNOWN-DEVICE", []byte("payload"))
	if !errors.Is(err, ErrNoSession) {
		t.Fatalf("got error %v, want ErrNoSession", err)
	}
}

func TestServiceSendToDeviceWithoutSessionStore(t *testing.T) {
	svc := NewService(nil, nil, nil, "", noopLogger{})
	svc.SetDeviceWriter(&fakeDeviceWriter{})

	if err := svc.SendToDevice("DEVICE123", []byte("payload")); err == nil {
		t.Fatalf("expected error when session store isn't configured")
	}
}

func TestServiceSendToDeviceWithoutDeviceWriter(t *testing.T) {
	sessions := NewSessionStore(time.Minute)
	svc := NewService(nil, nil, sessions, "", noopLogger{})

	addr := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 54321}
	sessions.Update("DEVICE123", addr)

	if err := svc.SendToDevice("DEVICE123", []byte("payload")); err == nil {
		t.Fatalf("expected error when device writer isn't configured")
	}
}

func TestServiceHandleForwarderReplyRelaysToKnownDevice(t *testing.T) {
	sessions := NewSessionStore(time.Minute)
	writer := &fakeDeviceWriter{}
	svc := NewService(nil, nil, sessions, "", noopLogger{})
	svc.SetDeviceWriter(writer)

	addr := &net.UDPAddr{IP: net.ParseIP("10.0.0.1"), Port: 54321}
	sessions.Update("DEVICE123", addr)

	reply := []byte("STT;DEVICE123;ack;payload")
	svc.HandleForwarderReply(reply)

	if writer.gotAddr.String() != addr.String() {
		t.Fatalf("got addr %s, want %s", writer.gotAddr, addr)
	}
	if string(writer.gotData) != string(reply) {
		t.Fatalf("got payload %q, want %q", writer.gotData, reply)
	}
}

func TestServiceHandleForwarderReplyDropsUnknownDevice(t *testing.T) {
	sessions := NewSessionStore(time.Minute)
	writer := &fakeDeviceWriter{}
	svc := NewService(nil, nil, sessions, "", noopLogger{})
	svc.SetDeviceWriter(writer)

	reply := []byte("STT;UNKNOWN-DEVICE;ack;payload")
	svc.HandleForwarderReply(reply) // must not panic, just logs and drops

	if writer.gotAddr != nil {
		t.Fatalf("expected no write for a device without a session")
	}
}

func TestServiceHandleForwarderReplyDropsPayloadWithoutDeviceIdentifier(t *testing.T) {
	sessions := NewSessionStore(time.Minute)
	writer := &fakeDeviceWriter{}
	svc := NewService(nil, nil, sessions, "", noopLogger{})
	svc.SetDeviceWriter(writer)

	svc.HandleForwarderReply([]byte("garbage payload with no known protocol marker"))

	if writer.gotAddr != nil {
		t.Fatalf("expected no write for a payload without a device identifier")
	}
}
