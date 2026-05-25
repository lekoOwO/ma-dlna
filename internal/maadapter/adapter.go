package maadapter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
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
	// Try primary service with MA-style payload first, then fallback with HA-native payload.
	if a.cfg.MAAdapter.PlayService != "" {
		maPayload := map[string]any{
			"entity_id":  targetEntity,
			"media_id":   contentID,
			"media_type": contentType,
		}
		if err := a.callHAService(a.cfg.MAAdapter.PlayService, maPayload); err == nil {
			return nil
		}
		slog.Warn("Primary play_media failed, trying with HA-native field names")
		// Also try primary with HA-native field names
		haPayload := map[string]any{
			"entity_id":          targetEntity,
			"media_content_id":   contentID,
			"media_content_type": contentType,
		}
		if err := a.callHAService(a.cfg.MAAdapter.PlayService, haPayload); err == nil {
			return nil
		}
	}
	if a.cfg.MAAdapter.FallbackPlayService != "" {
		haPayload := map[string]any{
			"entity_id":          targetEntity,
			"media_content_id":   contentID,
			"media_content_type": contentType,
		}
		return a.callHAService(a.cfg.MAAdapter.FallbackPlayService, haPayload)
	}
	return fmt.Errorf("no play service configured")
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

func bodyRedacted(body []byte) string {
	s := string(body)
	// Strip token=... from URLs in JSON
	for _, key := range []string{`"media_id":"`, `"media_content_id":"`} {
		if i := strings.Index(s, key); i >= 0 {
			start := i + len(key)
			if j := strings.IndexByte(s[start:], '"'); j >= 0 {
				url := s[start : start+j]
				s = s[:start] + sanitizeURL(url) + s[start+j:]
			}
		}
	}
	return s
}

func sanitizeURL(u string) string {
	if i := strings.IndexByte(u, '?'); i >= 0 {
		return u[:i] + "?token=***"
	}
	return u
}

func (a *Adapter) callHAService(service string, payload map[string]any) error {
	// HA REST API expects /api/services/{domain}/{service}, but config uses
	// domain.service notation (consistent with HA YAML). Replace last dot with /.
	apiPath := service
	if i := strings.LastIndex(service, "."); i > 0 {
		apiPath = service[:i] + "/" + service[i+1:]
	}
	url := fmt.Sprintf("%s/api/services/%s", a.cfg.HA.URL, apiPath)

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	slog.Debug("HA API call", "url", strings.Replace(url, a.cfg.HA.Token, "***", 1), "payload", bodyRedacted(body))

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
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		slog.Error("HA service error", "service", service, "status", resp.StatusCode, "body", sanitizeURL(string(body)))
		return fmt.Errorf("HA service %s returned status %d", service, resp.StatusCode)
	}

	slog.Info("HA service call succeeded", "service", service)
	return nil
}
