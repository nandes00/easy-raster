package tracker

import (
	"context"
	"net"
	"testing"
	"time"
)

func newUDPEcho(t *testing.T) *net.UDPConn {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("failed to start echo listener: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestUDPForwarderForwardAndListen(t *testing.T) {
	remote := newUDPEcho(t)

	fwd, err := NewUDPForwarder(remote.LocalAddr().String(), noopLogger{})
	if err != nil {
		t.Fatalf("failed to create forwarder: %v", err)
	}
	t.Cleanup(func() { fwd.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	received := make(chan []byte, 1)
	go fwd.Listen(ctx, func(payload []byte) {
		received <- payload
	})

	if err := fwd.Forward([]byte("hello")); err != nil {
		t.Fatalf("failed to forward: %v", err)
	}

	buf := make([]byte, 1024)
	remote.SetReadDeadline(time.Now().Add(time.Second))
	n, remoteAddr, err := remote.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("echo listener failed to read: %v", err)
	}
	if string(buf[:n]) != "hello" {
		t.Fatalf("got %q, want %q", buf[:n], "hello")
	}

	if _, err := remote.WriteToUDP([]byte("ack"), remoteAddr); err != nil {
		t.Fatalf("failed to reply: %v", err)
	}

	select {
	case payload := <-received:
		if string(payload) != "ack" {
			t.Fatalf("got %q, want %q", payload, "ack")
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for forwarder to relay reply")
	}
}

func TestUDPForwarderKeepalive(t *testing.T) {
	remote := newUDPEcho(t)

	fwd, err := NewUDPForwarder(remote.LocalAddr().String(), noopLogger{})
	if err != nil {
		t.Fatalf("failed to create forwarder: %v", err)
	}
	t.Cleanup(func() { fwd.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go fwd.Keepalive(ctx, 10*time.Millisecond)

	buf := make([]byte, 1024)
	remote.SetReadDeadline(time.Now().Add(time.Second))
	n, _, err := remote.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("echo listener didn't receive a keepalive packet: %v", err)
	}
	if n != len(natKeepalivePayload) {
		t.Fatalf("got %d bytes, want %d", n, len(natKeepalivePayload))
	}
}

func TestServerWriteToDevice(t *testing.T) {
	sessions := NewSessionStore(time.Minute)
	svc := NewService(nil, nil, sessions, "", noopLogger{})

	srv, err := NewServer("127.0.0.1:0", svc, noopLogger{})
	if err != nil {
		t.Fatalf("failed to create server: %v", err)
	}
	t.Cleanup(func() { srv.conn.Close() })

	client, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatalf("failed to create client socket: %v", err)
	}
	t.Cleanup(func() { client.Close() })

	clientAddr := client.LocalAddr().(*net.UDPAddr)
	if err := srv.WriteToDevice(clientAddr, []byte("command")); err != nil {
		t.Fatalf("failed to write to device: %v", err)
	}

	buf := make([]byte, 1024)
	client.SetReadDeadline(time.Now().Add(time.Second))
	n, _, err := client.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("client failed to read: %v", err)
	}
	if string(buf[:n]) != "command" {
		t.Fatalf("got %q, want %q", buf[:n], "command")
	}
}
