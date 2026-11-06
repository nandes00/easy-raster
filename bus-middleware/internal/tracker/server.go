package tracker

import (
	"context"
	"net"
	"time"
)

const (
	udpBufferSize    = 4096
	readDeadlineStep = time.Second
)

type Server struct {
	conn    *net.UDPConn
	service *Service
	logger  Logger
}

func NewServer(addr string, service *Service, logger Logger) (*Server, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}

	return &Server{
		conn:    conn,
		service: service,
		logger:  logger,
	}, nil
}

func (s *Server) Start(ctx context.Context) {
	defer s.conn.Close()

	s.logger.Info("UDP listener started", "addr", s.conn.LocalAddr().String())

	buf := make([]byte, udpBufferSize)

	for {
		s.conn.SetReadDeadline(time.Now().Add(readDeadlineStep))

		n, remoteAddr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				select {
				case <-ctx.Done():
					s.logger.Info("UDP listener shutting down")
					return
				default:
					continue
				}
			}
			s.logger.Error("failed to read UDP packet", "err", err)
			continue
		}

		payload := make([]byte, n)
		copy(payload, buf[:n])

		go func(addr *net.UDPAddr, data []byte) {
			s.service.Handle(ctx, addr, data)
		}(remoteAddr, payload)
	}
}

// WriteToDevice writes payload directly to addr on the listener socket —
// the same socket devices send to, so replies carry the address they
// expect (required for the device's NAT to accept them back).
func (s *Server) WriteToDevice(addr *net.UDPAddr, payload []byte) error {
	_, err := s.conn.WriteToUDP(payload, addr)
	return err
}
