package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"easyraster-middleware/internal/tracker"
)

const (
	defaultUDPAddr       = ":5023"
	sessionSweepInterval = time.Minute
	defaultNATKeepalive  = 15 * time.Second
)

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	udpAddr := envOr("UDP_ADDR", defaultUDPAddr)

	logLevel := slog.LevelInfo
	if envOr("LOG_LEVEL", "info") == "debug" {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var pub tracker.Publisher
	if queueURL := os.Getenv("SQS_QUEUE_URL"); queueURL != "" {
		p, err := tracker.NewSQSPublisher(ctx)
		if err != nil {
			logger.Error("failed to create SQS publisher", "err", err)
			os.Exit(1)
		}
		pub = p
		logger.Info("SQS publisher initialized", "queue_url", queueURL)
	} else {
		logger.Warn("SQS_QUEUE_URL not set — messages will not be published")
	}

	var fwd tracker.Forwarder
	if addr := os.Getenv("FORWARD_ADDR"); addr != "" {
		f, err := tracker.NewUDPForwarder(addr, logger)
		if err != nil {
			logger.Error("failed to create UDP forwarder", "addr", addr, "err", err)
			os.Exit(1)
		}
		fwd = f
		logger.Info("UDP forwarder initialized", "addr", addr)
	}

	sessionTTL, err := time.ParseDuration(os.Getenv("SESSION_TTL"))
	if err != nil {
		sessionTTL = 0 // NewSessionStore falls back to its default
	}
	sessions := tracker.NewSessionStore(sessionTTL)
	go sessions.Run(ctx, sessionSweepInterval)

	topic := os.Getenv("SQS_QUEUE_URL")
	svc := tracker.NewService(pub, fwd, sessions, topic, logger)

	srv, err := tracker.NewServer(udpAddr, svc, logger)
	if err != nil {
		logger.Error("failed to start UDP server", "addr", udpAddr, "err", err)
		os.Exit(1)
	}
	svc.SetDeviceWriter(srv)

	go srv.Start(ctx)

	if fwd != nil {
		natKeepaliveInterval, err := time.ParseDuration(os.Getenv("NAT_KEEPALIVE_INTERVAL"))
		if err != nil {
			natKeepaliveInterval = defaultNATKeepalive
		}
		go fwd.Listen(ctx, svc.HandleForwarderReply)
		go fwd.Keepalive(ctx, natKeepaliveInterval)
	}

	<-ctx.Done()
	logger.Info("shutting down")

	if pub != nil {
		if err := pub.Close(); err != nil {
			logger.Error("failed to close SQS publisher", "err", err)
		}
	}
	if fwd != nil {
		if err := fwd.Close(); err != nil {
			logger.Error("failed to close forwarder", "err", err)
		}
	}
}
