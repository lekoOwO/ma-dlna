package maadapter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	neturl "net/url"
	"path"
	"strings"
	"time"

	"github.com/leko/ma-dlna/internal/config"
)

type DirectAdapter struct {
	cfg    *config.Config
	client *http.Client
}

type maQueueStatus struct {
	State                  *string  `json:"state"`
	QueueID                *string  `json:"queue_id"`
	ElapsedTime            *float64 `json:"elapsed_time"`
	ElapsedTimeLastUpdated *float64 `json:"elapsed_time_last_updated"`
}

func newDirectAdapter(cfg *config.Config) *DirectAdapter {
	return &DirectAdapter{
		cfg: cfg,
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

func (a *DirectAdapter) Target() string {
	return a.cfg.MusicAssistant.TargetPlayerID
}

func (a *DirectAdapter) getActiveQueueStatus() (*maQueueStatus, error) {
	var result *maQueueStatus
	if err := a.callMAResult("player_queues/get_active_queue", map[string]any{
		"player_id": a.cfg.MusicAssistant.TargetPlayerID,
	}, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (a *DirectAdapter) resolveQueueID() (string, error) {
	result, err := a.getActiveQueueStatus()
	if err != nil {
		return "", err
	}
	if result != nil && result.QueueID != nil && *result.QueueID != "" {
		return *result.QueueID, nil
	}
	return a.cfg.MusicAssistant.TargetPlayerID, nil
}

func (a *DirectAdapter) PlayMedia(req PlayRequest) error {
	qid, err := a.resolveQueueID()
	if err != nil {
		return err
	}

	media := map[string]any{
		"item_id":    req.StreamURL,
		"provider":   "builtin",
		"name":       mediaName(req),
		"media_type": "track",
	}

	if req.Duration != "" {
		if secs := parseDurationSeconds(req.Duration); secs > 0 {
			media["duration"] = secs
		}
	}

	if req.Artist != "" {
		media["artists"] = []map[string]any{{
			"item_id":           req.Artist,
			"provider":          "builtin",
			"name":              req.Artist,
			"media_type":        "artist",
			"provider_mappings": providerMappings(req.Artist, ""),
		}}
	}
	if req.Album != "" {
		media["album"] = map[string]any{
			"item_id":           req.Album,
			"provider":          "builtin",
			"name":              req.Album,
			"media_type":        "album",
			"provider_mappings": providerMappings(req.Album, ""),
		}
	}
	if req.AlbumArtURI != "" {
		media["metadata"] = map[string]any{
			"images": []map[string]any{{
				"type":                "thumb",
				"path":                req.AlbumArtURI,
				"provider":            "builtin",
				"remotely_accessible": strings.HasPrefix(req.AlbumArtURI, "http://") || strings.HasPrefix(req.AlbumArtURI, "https://"),
			}},
		}
	}

	audioFmt := audioFormatFromMIME(req.ContentType)
	media["provider_mappings"] = providerMappings(req.StreamURL, audioFmt)

	return a.callMA("player_queues/play_media", map[string]any{
		"queue_id": qid,
		"media":    media,
		"option":   "replace",
	})
}

func providerMappings(itemID, contentType string) []map[string]any {
	mapping := map[string]any{
		"item_id":           itemID,
		"provider_domain":   "builtin",
		"provider_instance": "builtin",
	}
	if contentType != "" {
		mapping["audio_format"] = map[string]any{"content_type": contentType}
	}
	return []map[string]any{mapping}
}

func (a *DirectAdapter) Stop() error {
	qid, err := a.resolveQueueID()
	if err != nil {
		return err
	}
	return a.callMA("player_queues/stop", map[string]any{
		"queue_id": qid,
	})
}

func (a *DirectAdapter) Pause() error {
	qid, err := a.resolveQueueID()
	if err != nil {
		return err
	}
	return a.callMA("player_queues/pause", map[string]any{
		"queue_id": qid,
	})
}

func (a *DirectAdapter) SetVolume(volume int) error {
	return a.callMA("players/cmd/volume_set", map[string]any{
		"player_id":    a.cfg.MusicAssistant.TargetPlayerID,
		"volume_level": volume,
	})
}

func (a *DirectAdapter) GetState() (string, error) {
	ps, err := a.GetStatus()
	if err != nil {
		return "", err
	}
	return ps.State, nil
}

func (a *DirectAdapter) GetStatus() (PlayerStatus, error) {
	result, err := a.getActiveQueueStatus()
	if err != nil {
		return PlayerStatus{}, err
	}

	if result != nil && result.State != nil {
		return playerStatusFromMA(*result), nil
	}

	qid := a.cfg.MusicAssistant.TargetPlayerID
	if result != nil && result.QueueID != nil && *result.QueueID != "" {
		qid = *result.QueueID
	}

	var result2 *maQueueStatus
	if err := a.callMAResult("player_queues/get", map[string]any{
		"queue_id": qid,
	}, &result2); err != nil {
		return PlayerStatus{}, err
	}
	if result2 != nil {
		ps := playerStatusFromMA(*result2)
		if ps.QueueID == "" {
			ps.QueueID = qid
		}
		return ps, nil
	}
	return PlayerStatus{State: normalizeMAState("unknown")}, nil
}

func (a *DirectAdapter) PlaybackPosition() (time.Duration, bool, error) {
	status, err := a.GetStatus()
	if err != nil {
		return 0, false, err
	}
	if !status.HasElapsed {
		return 0, false, nil
	}
	elapsed := status.Elapsed
	if status.State == "playing" && !status.ElapsedUpdatedAt.IsZero() {
		if delta := time.Since(status.ElapsedUpdatedAt); delta >= 0 && delta < 10*time.Minute {
			elapsed += delta
		}
	}
	return elapsed, true, nil
}

func playerStatusFromMA(status maQueueStatus) PlayerStatus {
	ps := PlayerStatus{State: normalizeMAState("unknown")}
	if status.State != nil {
		ps.State = normalizeMAState(*status.State)
	}
	if status.QueueID != nil {
		ps.QueueID = *status.QueueID
	}
	if status.ElapsedTime != nil {
		ps.Elapsed = time.Duration(*status.ElapsedTime * float64(time.Second))
		ps.HasElapsed = true
	}
	if status.ElapsedTimeLastUpdated != nil {
		ps.ElapsedUpdatedAt = maTimestampToTime(*status.ElapsedTimeLastUpdated)
	}
	return ps
}

func maTimestampToTime(ts float64) time.Time {
	if ts <= 0 {
		return time.Time{}
	}
	if ts > 1e12 {
		ts /= 1000
	}
	if ts > 1e10 {
		return time.Time{}
	}
	sec := int64(ts)
	nsec := int64((ts - float64(sec)) * float64(time.Second))
	return time.Unix(sec, nsec)
}

func (a *DirectAdapter) callMA(command string, args map[string]any) error {
	return a.callMAResult(command, args, nil)
}

func (a *DirectAdapter) callMAResult(command string, args map[string]any, result any) error {
	body := map[string]any{
		"command": command,
	}
	if args != nil {
		body["args"] = args
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	url := strings.TrimRight(a.cfg.MusicAssistant.URL, "/") + "/api"

	slog.Debug("MA API call", "url", url, "command", command, "payload", redactMA(string(payload)))

	req, err := http.NewRequest("POST", url, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if a.cfg.MusicAssistant.Token != "" {
		req.Header.Set("Authorization", "Bearer "+a.cfg.MusicAssistant.Token)
	}

	resp, err := a.client.Do(req)
	if err != nil {
		slog.Error("MA API call failed", "command", command, "error", err)
		return fmt.Errorf("call MA %s: %w", command, err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		slog.Error("MA API error", "command", command, "status", resp.StatusCode, "body", redactMA(string(respBody)))
		return fmt.Errorf("MA %s returned status %d", command, resp.StatusCode)
	}

	respBody = bytes.TrimSpace(respBody)
	if len(respBody) == 0 {
		slog.Info("MA API call succeeded", "command", command)
		return nil
	}

	var envelope struct {
		Error  json.RawMessage `json:"error"`
		Result json.RawMessage `json:"result"`
	}
	if err := json.Unmarshal(respBody, &envelope); err != nil {
		return fmt.Errorf("decode MA response: %w", err)
	}
	if len(envelope.Error) > 0 && !bytes.Equal(bytes.TrimSpace(envelope.Error), []byte("null")) {
		slog.Error("MA API error payload", "command", command, "error", redactMA(string(envelope.Error)))
		return fmt.Errorf("MA %s error: %s", command, string(envelope.Error))
	}

	if result != nil {
		resultBody := respBody
		if len(envelope.Result) > 0 {
			resultBody = envelope.Result
		}
		if err := json.Unmarshal(resultBody, result); err != nil {
			return fmt.Errorf("decode MA result: %w", err)
		}
	}

	slog.Info("MA API call succeeded", "command", command)
	return nil
}

func normalizeMAState(state string) string {
	switch state {
	case "playing", "paused", "idle":
		return state
	case "unknown":
		return "unknown"
	default:
		return state
	}
}

func audioFormatFromMIME(mime string) string {
	switch mime {
	case "audio/mpeg":
		return "mp3"
	case "audio/ogg":
		return "ogg"
	case "audio/flac":
		return "flac"
	case "audio/aac":
		return "aac"
	case "audio/wav":
		return "wav"
	default:
		return "?"
	}
}

func mediaName(req PlayRequest) string {
	if req.Title != "" {
		return req.Title
	}
	if name := urlBaseName(req.SourceURL); name != "" {
		return name
	}
	if name := urlBaseName(req.StreamURL); name != "" {
		return name
	}
	return "DLNA Stream"
}

func urlBaseName(raw string) string {
	u, err := neturl.Parse(raw)
	if err != nil {
		return ""
	}
	name := path.Base(u.Path)
	if name == "." || name == "/" {
		return ""
	}
	return name
}

func parseDurationSeconds(s string) int {
	var h, m, sec int
	if _, err := fmt.Sscanf(s, "%d:%d:%d", &h, &m, &sec); err == nil {
		return h*3600 + m*60 + sec
	}
	return 0
}

func redactMA(s string) string {
	var b strings.Builder
	start := 0
	for {
		i := strings.Index(s[start:], "token=")
		if i < 0 {
			b.WriteString(s[start:])
			return b.String()
		}
		i += start
		valueStart := i + len("token=")
		valueEnd := valueStart
		for valueEnd < len(s) {
			switch s[valueEnd] {
			case '&', '"', '\'', ' ', '\\', '<', '>':
				goto redacted
			default:
				valueEnd++
			}
		}
	redacted:
		b.WriteString(s[start:valueStart])
		b.WriteString("***")
		start = valueEnd
	}
}
