package internal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

type ThirdPartyClient struct {
	enabled    bool
	vehicleURL string
	deviceURL  string
	http       *http.Client
}

func NewThirdPartyClient(enabled bool, vehicleURL, deviceURL string) *ThirdPartyClient {
	return &ThirdPartyClient{
		enabled:    enabled,
		vehicleURL: vehicleURL,
		deviceURL:  deviceURL,
		http:       &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *ThirdPartyClient) SyncVehicle(ctx context.Context, v *Vehicle) {
	if !c.enabled || c.vehicleURL == "" {
		return
	}
	if err := c.post(ctx, c.vehicleURL, v); err != nil {
		log.Printf("thirdparty: sync vehicle %s: %v", v.ID.Hex(), err)
	}
}

func (c *ThirdPartyClient) SyncDevice(ctx context.Context, d *Device) {
	if !c.enabled || c.deviceURL == "" {
		return
	}
	if err := c.post(ctx, c.deviceURL, d); err != nil {
		log.Printf("thirdparty: sync device %s: %v", d.ID.Hex(), err)
	}
}

func (c *ThirdPartyClient) post(ctx context.Context, url string, body any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return nil
}
