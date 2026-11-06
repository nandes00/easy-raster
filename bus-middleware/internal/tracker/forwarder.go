package tracker

import (
	"context"
	"net"
	"time"
)

// natKeepalivePayload is a minimal, non-empty UDP payload used only to keep
// the NAT Gateway mapping for the forwarder socket alive between forwarded
// packets. It doesn't match any tracker protocol and is ignored by the
// third party.
var natKeepalivePayload = []byte{0}

type Forwarder interface {
	Forward(payload []byte) error
	Addr() string
	// Listen reads replies (ACK/command) coming back on the forwarder
	// socket and invokes handler for each one, until ctx is cancelled.
	Listen(ctx context.Context, handler func(payload []byte))
	// Keepalive periodically sends a no-op packet on the forwarder socket
	// so its outbound NAT mapping doesn't expire while idle. This is
	// transitional: it only exists because of the bridge to the third
	// party's socket, and goes away once that bridge does.
	Keepalive(ctx context.Context, interval time.Duration)
	Close() error
}

type UDPForwarder struct {
	conn   *net.UDPConn
	addr   string
	logger Logger
}

func NewUDPForwarder(addr string, logger Logger) (*UDPForwarder, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}

	conn, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		return nil, err
	}

	return &UDPForwarder{conn: conn, addr: addr, logger: logger}, nil
}

func (f *UDPForwarder) Forward(payload []byte) error {
	_, err := f.conn.Write(payload)
	return err
}

func (f *UDPForwarder) Addr() string {
	return f.addr
}

func (f *UDPForwarder) Listen(ctx context.Context, handler func(payload []byte)) {
	buf := make([]byte, udpBufferSize)

	for {
		f.conn.SetReadDeadline(time.Now().Add(readDeadlineStep))

		n, err := f.conn.Read(buf)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}

			f.logger.Error("failed to read forwarder reply", "forward_addr", f.addr, "err", err)
			continue
		}

		payload := make([]byte, n)
		copy(payload, buf[:n])

		go handler(payload)
	}
}

func (f *UDPForwarder) Keepalive(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := f.conn.Write(natKeepalivePayload); err != nil {
				f.logger.Error("failed to send NAT keepalive", "forward_addr", f.addr, "err", err)
			}
		}
	}
}

func (f *UDPForwarder) Close() error {
	return f.conn.Close()
}
