package maadapter

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	neturl "net/url"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/leko/ma-dlna/internal/config"
)

const maxWebSocketPayloadBytes = 16 * 1024 * 1024

type DirectAdapter struct {
	cfg      *config.Config
	wsConn   net.Conn
	wsReader *bufio.Reader
	wsMu     sync.Mutex
	msgID    atomic.Int64
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

	mediaURL := req.SourceURL
	if mediaURL == "" {
		mediaURL = req.StreamURL
	}

	media := map[string]any{
		"item_id":    mediaURL,
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
	media["provider_mappings"] = mediaProviderMappings(mediaURL, audioFmt)

	return a.callMA("player_queues/play_media", map[string]any{
		"queue_id": qid,
		"media":    media,
		"option":   "replace",
	})
}

func mediaProviderMappings(itemID, contentType string) []map[string]any {
	mappings := providerMappings(itemID, contentType)
	mappings[0]["url"] = itemID
	return mappings
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

func (a *DirectAdapter) Resume() error {
	qid, err := a.resolveQueueID()
	if err != nil {
		return err
	}
	return a.callMA("player_queues/resume", map[string]any{
		"queue_id": qid,
	})
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

func (a *DirectAdapter) Seek(position time.Duration) error {
	qid, err := a.resolveQueueID()
	if err != nil {
		return err
	}
	return a.callMA("player_queues/seek", map[string]any{
		"queue_id": qid,
		"position": int(position.Seconds()),
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
	a.wsMu.Lock()
	defer a.wsMu.Unlock()

	if err := a.connectWSLocked(); err != nil {
		return err
	}
	if err := a.sendWSCommandLocked(command, args, result); err != nil {
		a.closeWSLocked()
		slog.Error("MA WebSocket call failed", "command", command, "error", err)
		return fmt.Errorf("call MA %s: %w", command, err)
	}
	slog.Info("MA WebSocket call succeeded", "command", command)
	return nil
}

func (a *DirectAdapter) connectWSLocked() error {
	if a.wsConn != nil {
		return nil
	}

	wsURL, err := musicAssistantWSURL(a.cfg.MusicAssistant.URL)
	if err != nil {
		return err
	}

	dialer := &net.Dialer{Timeout: 15 * time.Second}
	var conn net.Conn
	switch wsURL.Scheme {
	case "ws":
		conn, err = dialer.Dial("tcp", wsURL.Host)
	case "wss":
		conn, err = tls.DialWithDialer(dialer, "tcp", wsURL.Host, &tls.Config{ServerName: wsURL.Hostname()})
	default:
		err = fmt.Errorf("unsupported websocket scheme %q", wsURL.Scheme)
	}
	if err != nil {
		return fmt.Errorf("connect Music Assistant websocket: %w", err)
	}

	key, err := websocketKey()
	if err != nil {
		conn.Close()
		return err
	}
	pathAndQuery := wsURL.RequestURI()
	if pathAndQuery == "" {
		pathAndQuery = "/ws"
	}
	req, err := http.NewRequest(http.MethodGet, wsURL.String(), nil)
	if err != nil {
		conn.Close()
		return err
	}
	req.Host = wsURL.Host
	req.URL.Scheme = ""
	req.URL.Host = ""
	req.RequestURI = pathAndQuery
	req.Header.Set("Host", wsURL.Host)
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Sec-WebSocket-Key", key)
	req.Header.Set("Sec-WebSocket-Version", "13")

	if err := req.Write(conn); err != nil {
		conn.Close()
		return fmt.Errorf("send websocket handshake: %w", err)
	}

	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		conn.Close()
		return fmt.Errorf("read websocket handshake: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		conn.Close()
		return fmt.Errorf("Music Assistant websocket upgrade returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	wantAccept := websocketAcceptKey(key)
	if got := resp.Header.Get("Sec-WebSocket-Accept"); got != wantAccept {
		conn.Close()
		return fmt.Errorf("invalid websocket accept header")
	}

	a.wsConn = conn
	a.wsReader = br

	if a.cfg.MusicAssistant.Token != "" {
		var auth struct {
			Authenticated bool `json:"authenticated"`
		}
		if err := a.sendWSCommandLocked("auth", map[string]any{"token": a.cfg.MusicAssistant.Token}, &auth); err != nil {
			a.closeWSLocked()
			return fmt.Errorf("authenticate Music Assistant websocket: %w", err)
		}
		if !auth.Authenticated {
			a.closeWSLocked()
			return fmt.Errorf("Music Assistant websocket authentication rejected")
		}
	}

	slog.Info("Connected to Music Assistant WebSocket", "url", wsURL.Redacted())
	return nil
}

func musicAssistantWSURL(raw string) (*neturl.URL, error) {
	u, err := neturl.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("parse music_assistant.url: %w", err)
	}
	if u.Scheme == "" || u.Host == "" {
		return nil, fmt.Errorf("music_assistant.url must have a scheme and host, got %q", raw)
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
	default:
		return nil, fmt.Errorf("music_assistant.url scheme must be http, https, ws, or wss, got %q", u.Scheme)
	}
	cleanPath := strings.TrimRight(u.Path, "/")
	if cleanPath == "" {
		u.Path = "/ws"
	} else if cleanPath == "/ws" {
		u.Path = cleanPath
	} else {
		u.Path = cleanPath + "/ws"
	}
	u.RawQuery = ""
	u.Fragment = ""
	return u, nil
}

func websocketKey() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate websocket key: %w", err)
	}
	return base64.StdEncoding.EncodeToString(b[:]), nil
}

func websocketAcceptKey(key string) string {
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(sum[:])
}

func (a *DirectAdapter) sendWSCommandLocked(command string, args map[string]any, result any) error {
	if a.wsConn == nil || a.wsReader == nil {
		return fmt.Errorf("Music Assistant websocket is not connected")
	}

	msgID := fmt.Sprintf("ma-dlna-%d", a.msgID.Add(1))
	req := map[string]any{
		"message_id": msgID,
		"command":    command,
	}
	if args != nil {
		req["args"] = args
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal websocket request: %w", err)
	}

	slog.Debug("MA WebSocket call", "command", command, "payload", redactMA(string(payload)))
	if err := a.wsConn.SetDeadline(time.Now().Add(15 * time.Second)); err != nil {
		return err
	}
	if err := writeClientTextFrame(a.wsConn, payload); err != nil {
		return fmt.Errorf("send websocket request: %w", err)
	}

	for {
		msg, err := a.readWSTextLocked()
		if err != nil {
			return err
		}
		var envelope struct {
			MessageID string          `json:"message_id"`
			Result    json.RawMessage `json:"result"`
			Error     json.RawMessage `json:"error"`
			ErrorCode json.RawMessage `json:"error_code"`
			Details   json.RawMessage `json:"details"`
			Event     string          `json:"event"`
		}
		if err := json.Unmarshal(msg, &envelope); err != nil {
			return fmt.Errorf("decode websocket response: %w", err)
		}
		if envelope.Event != "" {
			// Event handling is added in the state-sync layer; commands must ignore
			// unsolicited events while waiting for their matching response.
			continue
		}
		if envelope.MessageID == "" || envelope.MessageID != msgID {
			continue
		}
		if len(envelope.ErrorCode) > 0 && string(envelope.ErrorCode) != "null" {
			code := errorCodeString(envelope.ErrorCode)
			if len(envelope.Details) > 0 {
				return fmt.Errorf("MA websocket error %s: %s", code, string(envelope.Details))
			}
			return fmt.Errorf("MA websocket error %s", code)
		}
		if len(envelope.Error) > 0 && string(envelope.Error) != "null" {
			return fmt.Errorf("MA websocket error: %s", string(envelope.Error))
		}
		if result != nil && len(envelope.Result) > 0 && string(envelope.Result) != "null" {
			if err := json.Unmarshal(envelope.Result, result); err != nil {
				return fmt.Errorf("decode MA result: %w", err)
			}
		}
		return nil
	}
}

func (a *DirectAdapter) readWSTextLocked() ([]byte, error) {
	for {
		fin, opcode, payload, err := readServerFrame(a.wsReader)
		if err != nil {
			return nil, fmt.Errorf("read websocket frame: %w", err)
		}
		switch opcode {
		case 0x1:
			if fin {
				return payload, nil
			}
			var buf bytes.Buffer
			buf.Write(payload)
			for {
				nextFin, nextOpcode, nextPayload, err := readServerFrame(a.wsReader)
				if err != nil {
					return nil, fmt.Errorf("read websocket continuation: %w", err)
				}
				switch nextOpcode {
				case 0x0:
					if buf.Len()+len(nextPayload) > maxWebSocketPayloadBytes {
						return nil, fmt.Errorf("websocket message too large")
					}
					buf.Write(nextPayload)
					if nextFin {
						return buf.Bytes(), nil
					}
				case 0x8:
					return nil, io.EOF
				case 0x9:
					if err := writeClientControlFrame(a.wsConn, 0xA, nextPayload); err != nil {
						return nil, err
					}
				}
			}
		case 0x8:
			return nil, io.EOF
		case 0x9:
			if err := writeClientControlFrame(a.wsConn, 0xA, payload); err != nil {
				return nil, err
			}
		case 0xA:
			continue
		default:
			continue
		}
	}
}

func errorCodeString(raw json.RawMessage) string {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	return strings.TrimSpace(string(raw))
}

func readServerFrame(r *bufio.Reader) (bool, byte, []byte, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(r, header); err != nil {
		return false, 0, nil, err
	}
	fin := header[0]&0x80 != 0
	opcode := header[0] & 0x0f
	masked := header[1]&0x80 != 0
	n := uint64(header[1] & 0x7f)
	switch n {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return false, 0, nil, err
		}
		n = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return false, 0, nil, err
		}
		n = binary.BigEndian.Uint64(ext[:])
	}
	if n > maxWebSocketPayloadBytes {
		return false, 0, nil, fmt.Errorf("websocket frame too large: %d bytes", n)
	}
	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(r, mask[:]); err != nil {
			return false, 0, nil, err
		}
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return false, 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return fin, opcode, payload, nil
}

func writeClientTextFrame(w io.Writer, payload []byte) error {
	return writeClientFrame(w, 0x1, payload)
}

func writeClientControlFrame(w io.Writer, opcode byte, payload []byte) error {
	if len(payload) > 125 {
		payload = payload[:125]
	}
	return writeClientFrame(w, opcode, payload)
}

func writeClientFrame(w io.Writer, opcode byte, payload []byte) error {
	var header []byte
	header = append(header, 0x80|opcode)
	n := len(payload)
	switch {
	case n < 126:
		header = append(header, 0x80|byte(n))
	case n <= 65535:
		header = append(header, 0x80|126)
		var ext [2]byte
		binary.BigEndian.PutUint16(ext[:], uint16(n))
		header = append(header, ext[:]...)
	default:
		header = append(header, 0x80|127)
		var ext [8]byte
		binary.BigEndian.PutUint64(ext[:], uint64(n))
		header = append(header, ext[:]...)
	}
	var mask [4]byte
	if _, err := rand.Read(mask[:]); err != nil {
		return err
	}
	header = append(header, mask[:]...)
	masked := make([]byte, len(payload))
	for i := range payload {
		masked[i] = payload[i] ^ mask[i%4]
	}
	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(masked)
	return err
}

func (a *DirectAdapter) closeWSLocked() {
	if a.wsConn != nil {
		_ = a.wsConn.Close()
	}
	a.wsConn = nil
	a.wsReader = nil
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
	var v any
	if err := json.Unmarshal([]byte(s), &v); err == nil {
		redactValue(v)
		if out, err := json.Marshal(v); err == nil {
			return string(out)
		}
	}
	return redactLogString(s)
}

func redactValue(v any) {
	switch x := v.(type) {
	case map[string]any:
		for k, val := range x {
			lower := strings.ToLower(k)
			if strings.Contains(lower, "token") || strings.Contains(lower, "password") || strings.Contains(lower, "secret") {
				x[k] = "***"
				continue
			}
			if s, ok := val.(string); ok {
				x[k] = redactLogString(s)
				continue
			}
			redactValue(val)
		}
	case []any:
		for _, val := range x {
			redactValue(val)
		}
	}
}

func redactLogString(s string) string {
	u, err := neturl.Parse(s)
	if err == nil && u.Scheme != "" && u.Host != "" {
		switch u.Scheme {
		case "http", "https", "ws", "wss":
			if u.RawQuery != "" {
				u.RawQuery = "..."
			}
			if u.User != nil {
				u.User = neturl.User("***")
			}
			return u.String()
		}
	}
	if strings.Contains(s, "token=") {
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
	return s
}
