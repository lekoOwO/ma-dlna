package maadapter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/leko/ma-dlna/internal/config"
)

type Adapter struct {
	cfg    *config.Config
	client *http.Client
}

func New(cfg *config.Config) *Adapter {
	return &Adapter{
		cfg: cfg,
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

func (a *Adapter) PlayMedia(targetEntity, contentID, contentType string) error {
	payload := map[string]any{
		"entity_id":          targetEntity,
		"media_content_id":   contentID,
		"media_content_type": contentType,
	}
	return a.callWithFallback(a.cfg.MAAdapter.PlayService, a.cfg.MAAdapter.FallbackPlayService, payload)
}

func (a *Adapter) Stop(targetEntity string) error {
	return a.callHAService(a.cfg.MAAdapter.StopService, map[string]any{
		"entity_id": targetEntity,
	})
}

func (a *Adapter) Pause(targetEntity string) error {
	return a.callHAService(a.cfg.MAAdapter.PauseService, map[string]any{
		"entity_id": targetEntity,
	})
}

func (a *Adapter) SetVolume(targetEntity string, volume int) error {
	return a.callHAService(a.cfg.MAAdapter.VolumeService, map[string]any{
		"entity_id":    targetEntity,
		"volume_level": float64(volume) / 100.0,
	})
}

func (a *Adapter) callWithFallback(primary, fallback string, payload map[string]any) error {
	if primary != "" {
		err := a.callHAService(primary, payload)
		if err == nil {
			return nil
		}
		slog.Warn("Primary service failed, trying fallback", "primary", primary, "fallback", fallback)
	}
	if fallback != "" {
		return a.callHAService(fallback, payload)
	}
	return fmt.Errorf("no service configured")
}

func (a *Adapter) callHAService(service string, payload map[string]any) error {
	url := fmt.Sprintf("%s/api/services/%s", a.cfg.HA.URL, service)

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	slog.Debug("HA API call", "url", url, "payload", string(body))

	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+a.cfg.HA.Token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.client.Do(req)
	if err != nil {
		slog.Error("HA service call failed", "service", service, "error", err)
		return fmt.Errorf("call HA service %s: %w", service, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		slog.Error("HA service returned error status", "service", service, "status", resp.StatusCode)
		return fmt.Errorf("HA service %s returned status %d", service, resp.StatusCode)
	}

	slog.Info("HA service call succeeded", "service", service)
	return nil
}
