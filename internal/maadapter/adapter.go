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

// PlayerStatus holds enriched player state from the backend.
type PlayerStatus struct {
	State            string
	QueueID          string
	Elapsed          time.Duration
	ElapsedUpdatedAt time.Time
	HasElapsed       bool
}

// PlayerClient is the interface for controlling a media player backend.
type PlayerClient interface {
	Target() string
	RequiresBridgeStream() bool
	PlayMedia(req PlayRequest) error
	Resume() error
	Stop() error
	Pause() error
	Seek(position time.Duration) error
	SetVolume(volume int) error
	GetState() (string, error)
	GetStatus() (PlayerStatus, error)
	PlaybackPosition() (time.Duration, bool, error)
}

// PlayRequest carries the stream and metadata to send to the player backend.
type PlayRequest struct {
	StreamURL   string
	SourceURL   string
	ContentType string
	Title       string
	Artist      string
	Album       string
	AlbumArtURI string
	Duration    string
}

// New returns a PlayerClient for the configured mode.
func New(cfg *config.Config) PlayerClient {
	switch cfg.MAAdapter.Mode {
	case "direct":
		return newDirectAdapter(cfg)
	default:
		return newHAAdapter(cfg)
	}
}

type HAAdapter struct {
	cfg    *config.Config
	client *http.Client
}

func newHAAdapter(cfg *config.Config) *HAAdapter {
	return &HAAdapter{
		cfg: cfg,
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

func (a *HAAdapter) Target() string {
	return a.cfg.HA.TargetEntityID
}

func (a *HAAdapter) RequiresBridgeStream() bool {
	return true
}

func (a *HAAdapter) PlayMedia(req PlayRequest) error {
	targetEntity := a.cfg.HA.TargetEntityID
	contentID := req.StreamURL
	contentType := req.ContentType

	if a.cfg.MAAdapter.PlayService != "" {
		maPayload := map[string]any{
			"entity_id": targetEntity,
			"media_id":  contentID,
		}
		if validMAType(contentType) {
			maPayload["media_type"] = contentType
		}
		if err := a.callHAService(a.cfg.MAAdapter.PlayService, maPayload); err == nil {
			return nil
		}
		slog.Warn("Primary play_media failed, trying with HA-native field names")
		haPayload := map[string]any{
			"entity_id":        targetEntity,
			"media_content_id": contentID,
		}
		if validMAType(contentType) {
			haPayload["media_content_type"] = contentType
		} else {
			haPayload["media_content_type"] = "music"
		}
		if err := a.callHAService(a.cfg.MAAdapter.PlayService, haPayload); err == nil {
			return nil
		}
	}
	if a.cfg.MAAdapter.FallbackPlayService != "" {
		haPayload := map[string]any{
			"entity_id":          targetEntity,
			"media_content_id":   contentID,
			"media_content_type": haMediaType(contentType),
		}
		return a.callHAService(a.cfg.MAAdapter.FallbackPlayService, haPayload)
	}
	return fmt.Errorf("no play service configured")
}

func (a *HAAdapter) Resume() error {
	return nil
}

func (a *HAAdapter) Stop() error {
	return a.callHAService(a.cfg.MAAdapter.StopService, map[string]any{
		"entity_id": a.cfg.HA.TargetEntityID,
	})
}

func (a *HAAdapter) Pause() error {
	return a.callHAService(a.cfg.MAAdapter.PauseService, map[string]any{
		"entity_id": a.cfg.HA.TargetEntityID,
	})
}

func (a *HAAdapter) Seek(position time.Duration) error {
	return nil
}

func (a *HAAdapter) SetVolume(volume int) error {
	return a.callHAService(a.cfg.MAAdapter.VolumeService, map[string]any{
		"entity_id":    a.cfg.HA.TargetEntityID,
		"volume_level": float64(volume) / 100.0,
	})
}

func (a *HAAdapter) GetState() (string, error) {
	return a.getEntityState(a.cfg.HA.TargetEntityID)
}

func (a *HAAdapter) GetStatus() (PlayerStatus, error) {
	state, err := a.getEntityState(a.cfg.HA.TargetEntityID)
	if err != nil {
		return PlayerStatus{}, err
	}
	return PlayerStatus{State: state}, nil
}

func (a *HAAdapter) PlaybackPosition() (time.Duration, bool, error) {
	return 0, false, nil
}

func (a *HAAdapter) getEntityState(entityID string) (string, error) {
	url := fmt.Sprintf("%s/api/states/%s", a.cfg.HA.URL, entityID)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+a.cfg.HA.Token)
	resp, err := a.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("get entity state: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("get entity state returned %d", resp.StatusCode)
	}
	var result struct {
		State string `json:"state"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode entity state: %w", err)
	}
	return result.State, nil
}

func validMAType(contentType string) bool {
	switch contentType {
	case "artist", "album", "track", "playlist", "radio":
		return true
	}
	return false
}

func haMediaType(contentType string) string {
	if validMAType(contentType) {
		return contentType
	}
	return "music"
}

func (a *HAAdapter) callHAService(service string, payload map[string]any) error {
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

func bodyRedacted(body []byte) string {
	s := string(body)
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
