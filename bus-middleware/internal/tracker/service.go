package tracker

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"time"

	"github.com/google/uuid"
)

type Logger interface {
	Info(msg string, args ...any)
	Error(msg string, args ...any)
	Debug(msg string, args ...any)
}

// ErrNoSession is returned by Service.SendToDevice when there's no known
// address for the given device — the session expired or the device was
// never seen by this instance.
var ErrNoSession = errors.New("no active session for device")

// DeviceWriter writes a payload back to a device at addr, using the same
// listener socket that receives packets from devices.
type DeviceWriter interface {
	WriteToDevice(addr *net.UDPAddr, payload []byte) error
}

type rawEvent struct {
	MessageID        string `json:"messageId"`
	ReceivedAt       string `json:"receivedAt"`
	Protocol         string `json:"protocol"`
	DeviceType       string `json:"deviceType"`
	Serial           string `json:"serial"`
	DeviceIdentifier string `json:"deviceIdentifier"`
	RawPayload       string `json:"rawPayload"`
}

type Service struct {
	publisher    Publisher
	forwarder    Forwarder
	sessions     *SessionStore
	deviceWriter DeviceWriter
	topic        string
	logger       Logger
}

func NewService(pub Publisher, fwd Forwarder, sessions *SessionStore, topic string, logger Logger) *Service {
	return &Service{publisher: pub, forwarder: fwd, sessions: sessions, topic: topic, logger: logger}
}

// SetDeviceWriter wires the component that can write back to a device's
// listener socket. Separate from NewService because the writer (the UDP
// server) is constructed after the Service that it depends on.
func (s *Service) SetDeviceWriter(w DeviceWriter) {
	s.deviceWriter = w
}

// SendToDevice relays payload to the device identified by deviceIdentifier,
// using the last known address recorded in the session table. This is the
// single entry point for both the ACK/command bridge from the forwarder
// today and, eventually, internally generated ACKs/commands — only the
// caller changes.
func (s *Service) SendToDevice(deviceIdentifier string, payload []byte) error {
	if s.sessions == nil {
		return errors.New("session store not configured")
	}

	addr, ok := s.sessions.Lookup(deviceIdentifier)
	if !ok {
		return ErrNoSession
	}

	if s.deviceWriter == nil {
		return errors.New("device writer not configured")
	}

	return s.deviceWriter.WriteToDevice(addr, payload)
}

// HandleForwarderReply processes an ACK or out-of-band command read back
// from the forwarder socket and relays it to the originating device.
func (s *Service) HandleForwarderReply(payload []byte) {
	s.logger.Debug("forwarder reply received", "size", len(payload), "payload", string(payload))

	fields := extractFields(payload)
	s.logger.Debug("forwarder reply parsed", "protocol", fields.Protocol, "device_type", fields.DeviceType, "device_identifier", fields.DeviceIdentifier)

	if fields.DeviceIdentifier == "" {
		s.logger.Error("forwarder reply without device identifier, dropping", "size", len(payload))
		return
	}

	if err := s.SendToDevice(fields.DeviceIdentifier, payload); err != nil {
		if errors.Is(err, ErrNoSession) {
			s.logger.Error("forwarder reply dropped: no active session for device", "device_identifier", fields.DeviceIdentifier)
			return
		}
		s.logger.Error("failed to relay forwarder reply to device", "device_identifier", fields.DeviceIdentifier, "err", err)
		return
	}

	s.logger.Debug("forwarder reply relayed to device", "device_identifier", fields.DeviceIdentifier)
}

func (s *Service) Handle(ctx context.Context, remoteAddr *net.UDPAddr, payload []byte) {
	remoteAddrStr := remoteAddr.String()
	s.logger.Info("packet received", "remote_addr", remoteAddrStr, "size", len(payload))
	s.logger.Debug("packet payload", "remote_addr", remoteAddrStr, "payload", string(payload))

	fields := extractFields(payload)
	s.logger.Debug("packet parsed", "protocol", fields.Protocol, "device_type", fields.DeviceType, "device_identifier", fields.DeviceIdentifier)

	if s.sessions != nil && fields.DeviceIdentifier != "" {
		s.sessions.Update(fields.DeviceIdentifier, remoteAddr)
	}

	if s.forwarder != nil {
		if err := s.forwarder.Forward(payload); err != nil {
			s.logger.Error("failed to forward packet", "remote_addr", remoteAddrStr, "forward_addr", s.forwarder.Addr(), "err", err)
		} else {
			s.logger.Debug("packet forwarded", "remote_addr", remoteAddrStr, "forward_addr", s.forwarder.Addr())
		}
	}

	if s.publisher == nil {
		return
	}

	event := rawEvent{
		MessageID:        uuid.New().String(),
		ReceivedAt:       time.Now().UTC().Format(time.RFC3339Nano),
		Protocol:         fields.Protocol,
		DeviceType:       fields.DeviceType,
		Serial:           fields.Serial,
		DeviceIdentifier: fields.DeviceIdentifier,
		RawPayload:       string(payload),
	}

	data, err := json.Marshal(event)
	if err != nil {
		s.logger.Error("failed to marshal event", "err", err)
		return
	}

	partitionKey := fields.DeviceIdentifier
	if partitionKey == "" {
		partitionKey = remoteAddrStr
	}

	if err := s.publisher.Publish(ctx, s.topic, []byte(partitionKey), data); err != nil {
		s.logger.Error("failed to publish to kafka", "topic", s.topic, "err", err)
		return
	}

	s.logger.Debug("packet published to kafka", "topic", s.topic, "messageId", event.MessageID)
}
