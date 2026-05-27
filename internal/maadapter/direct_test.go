package maadapter

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/leko/ma-dlna/internal/config"
)

type maWSCommand struct {
	MessageID string         `json:"message_id"`
	Command   string         `json:"command"`
	Args      map[string]any `json:"args"`
}

type maWSReply struct {
	Result string
	Close  bool
}

func directCfg() config.Config {
	cfg := config.DefaultConfig()
	cfg.MusicAssistant.Token = "ma-token"
	cfg.MusicAssistant.TargetPlayerID = "player123"
	return cfg
}

func directAdapter(cfg *config.Config, srvURL string) *DirectAdapter {
	cfg.MusicAssistant.URL = srvURL
	return newDirectAdapter(cfg)
}

func newMAWSServer(t *testing.T, handle func(maWSCommand) maWSReply) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ws" {
			t.Fatalf("unexpected websocket path: %s", r.URL.Path)
		}
		conn, rw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Fatalf("hijack websocket: %v", err)
		}
		defer conn.Close()

		_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\n")
		_, _ = rw.WriteString("Upgrade: websocket\r\n")
		_, _ = rw.WriteString("Connection: Upgrade\r\n")
		_, _ = rw.WriteString("Sec-WebSocket-Accept: " + websocketAccept(r.Header.Get("Sec-WebSocket-Key")) + "\r\n\r\n")
		if err := rw.Flush(); err != nil {
			t.Fatalf("flush websocket upgrade: %v", err)
		}

		writeServerText(t, rw, `{"server_id":"test","server_version":"2.8.8"}`)
		for {
			msg, err := readClientText(rw.Reader)
			if err != nil {
				return
			}
			var req maWSCommand
			if err := json.Unmarshal([]byte(msg), &req); err != nil {
				t.Fatalf("decode websocket request %q: %v", msg, err)
			}
			reply := handle(req)
			result := reply.Result
			if result == "" {
				result = "null"
			}
			writeServerText(t, rw, `{"message_id":"`+req.MessageID+`","result":`+result+`,"partial":false}`)
			if reply.Close {
				return
			}
		}
	}))
}

func expectAuth(t *testing.T, req maWSCommand) bool {
	t.Helper()
	if req.Command != "auth" {
		return false
	}
	if req.Args["token"] != "ma-token" {
		t.Fatalf("unexpected auth token: %v", req.Args["token"])
	}
	return true
}

func TestDirectPlayMediaPayload(t *testing.T) {
	cfg := directCfg()
	var playArgs map[string]any
	sourceURL := "http://source.local/path/song.mp3?token=source"
	server := newMAWSServer(t, func(req maWSCommand) maWSReply {
		switch req.Command {
		case "auth":
			expectAuth(t, req)
			return maWSReply{Result: `{"authenticated":true}`}
		case "player_queues/get_active_queue":
			if req.Args["player_id"] != "player123" {
				t.Fatalf("unexpected player_id: %v", req.Args["player_id"])
			}
			return maWSReply{Result: `{"queue_id":"group123","state":"idle"}`}
		case "player_queues/play_media":
			playArgs = req.Args
			return maWSReply{Result: `null`, Close: true}
		default:
			t.Fatalf("unexpected command: %s", req.Command)
			return maWSReply{}
		}
	})
	defer server.Close()

	adapter := directAdapter(&cfg, server.URL)
	err := adapter.PlayMedia(PlayRequest{
		StreamURL:   "http://bridge:8787/live/test.opus?token=abc",
		SourceURL:   sourceURL,
		ContentType: "audio/ogg",
		Title:       "Test Song",
		Artist:      "Test Artist",
		Album:       "Test Album",
		AlbumArtURI: "http://images.local/cover.jpg",
		Duration:    "00:04:06",
	})
	if err != nil {
		t.Fatalf("PlayMedia failed: %v", err)
	}

	if playArgs["queue_id"] != "group123" {
		t.Errorf("unexpected queue_id: %v", playArgs["queue_id"])
	}
	if playArgs["option"] != "replace" {
		t.Errorf("unexpected option: %v", playArgs["option"])
	}
	media := playArgs["media"].(map[string]any)
	if media["item_id"] != sourceURL {
		t.Fatalf("direct mode should send source URL as item_id, got %v", media["item_id"])
	}
	if media["provider"] != "builtin" || media["name"] != "Test Song" || media["media_type"] != "track" {
		t.Fatalf("unexpected media identity: %#v", media)
	}
	if media["duration"].(float64) != 246 {
		t.Errorf("unexpected duration: %v", media["duration"])
	}
	pm := media["provider_mappings"].([]any)[0].(map[string]any)
	if pm["item_id"] != sourceURL || pm["url"] != sourceURL {
		t.Fatalf("provider mapping should use source URL, got %#v", pm)
	}
	if pm["audio_format"].(map[string]any)["content_type"] != "ogg" {
		t.Fatalf("expected ogg audio format, got %#v", pm["audio_format"])
	}
	artist := media["artists"].([]any)[0].(map[string]any)
	if artist["name"] != "Test Artist" {
		t.Errorf("unexpected artist: %#v", artist)
	}
	album := media["album"].(map[string]any)
	if album["name"] != "Test Album" {
		t.Errorf("unexpected album: %#v", album)
	}
	image := media["metadata"].(map[string]any)["images"].([]any)[0].(map[string]any)
	if image["path"] != "http://images.local/cover.jpg" {
		t.Errorf("unexpected album art: %#v", image)
	}
}

func TestDirectPlayMediaNameFallbackAndAudioFormatMapping(t *testing.T) {
	tests := []struct {
		mime        string
		expectedFmt string
	}{
		{"audio/mpeg", "mp3"},
		{"audio/ogg", "ogg"},
		{"audio/flac", "flac"},
		{"audio/aac", "aac"},
		{"audio/wav", "wav"},
		{"audio/unknown", "?"},
	}
	for _, tc := range tests {
		t.Run(tc.mime, func(t *testing.T) {
			cfg := directCfg()
			var media map[string]any
			server := newMAWSServer(t, func(req maWSCommand) maWSReply {
				switch req.Command {
				case "auth":
					expectAuth(t, req)
					return maWSReply{Result: `{"authenticated":true}`}
				case "player_queues/get_active_queue":
					return maWSReply{Result: `{"queue_id":"group123","state":"idle"}`}
				case "player_queues/play_media":
					media = req.Args["media"].(map[string]any)
					return maWSReply{Close: true}
				default:
					t.Fatalf("unexpected command: %s", req.Command)
					return maWSReply{}
				}
			})
			defer server.Close()

			adapter := directAdapter(&cfg, server.URL)
			if err := adapter.PlayMedia(PlayRequest{
				SourceURL:   "http://source.local/path/song.flac?token=source",
				ContentType: tc.mime,
			}); err != nil {
				t.Fatalf("PlayMedia failed: %v", err)
			}
			if media["name"] != "song.flac" {
				t.Errorf("name should fallback to source URL basename, got %v", media["name"])
			}
			pm := media["provider_mappings"].([]any)[0].(map[string]any)
			if pm["audio_format"].(map[string]any)["content_type"] != tc.expectedFmt {
				t.Errorf("expected %q, got %q", tc.expectedFmt, pm["audio_format"].(map[string]any)["content_type"])
			}
		})
	}
}

func TestDirectQueueCommandsUseResolvedQueue(t *testing.T) {
	tests := []struct {
		name    string
		run     func(*DirectAdapter) error
		command string
		check   func(*testing.T, map[string]any)
	}{
		{"stop", (*DirectAdapter).Stop, "player_queues/stop", nil},
		{"pause", (*DirectAdapter).Pause, "player_queues/pause", nil},
		{"resume", (*DirectAdapter).Resume, "player_queues/resume", nil},
		{"seek", func(a *DirectAdapter) error { return a.Seek(42 * time.Second) }, "player_queues/seek", func(t *testing.T, args map[string]any) {
			if args["position"].(float64) != 42 {
				t.Fatalf("unexpected seek position: %v", args["position"])
			}
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := directCfg()
			var commandArgs map[string]any
			server := newMAWSServer(t, func(req maWSCommand) maWSReply {
				switch req.Command {
				case "auth":
					expectAuth(t, req)
					return maWSReply{Result: `{"authenticated":true}`}
				case "player_queues/get_active_queue":
					return maWSReply{Result: `{"queue_id":"group123","state":"playing"}`}
				case tc.command:
					commandArgs = req.Args
					return maWSReply{Close: true}
				default:
					t.Fatalf("unexpected command: %s", req.Command)
					return maWSReply{}
				}
			})
			defer server.Close()

			adapter := directAdapter(&cfg, server.URL)
			if err := tc.run(adapter); err != nil {
				t.Fatalf("%s failed: %v", tc.name, err)
			}
			if commandArgs["queue_id"] != "group123" {
				t.Fatalf("unexpected queue_id: %v", commandArgs["queue_id"])
			}
			if tc.check != nil {
				tc.check(t, commandArgs)
			}
		})
	}
}

func TestDirectSetVolume(t *testing.T) {
	cfg := directCfg()
	var args map[string]any
	server := newMAWSServer(t, func(req maWSCommand) maWSReply {
		switch req.Command {
		case "auth":
			expectAuth(t, req)
			return maWSReply{Result: `{"authenticated":true}`}
		case "players/cmd/volume_set":
			args = req.Args
			return maWSReply{Close: true}
		default:
			t.Fatalf("unexpected command: %s", req.Command)
			return maWSReply{}
		}
	})
	defer server.Close()

	adapter := directAdapter(&cfg, server.URL)
	if err := adapter.SetVolume(75); err != nil {
		t.Fatalf("SetVolume failed: %v", err)
	}
	if args["player_id"] != "player123" || args["volume_level"].(float64) != 75 {
		t.Fatalf("unexpected volume args: %#v", args)
	}
}

func TestDirectGetStatusAndPlaybackPosition(t *testing.T) {
	cfg := directCfg()
	updatedAt := time.Now().Add(-2 * time.Second)
	server := newMAWSServer(t, func(req maWSCommand) maWSReply {
		switch req.Command {
		case "auth":
			expectAuth(t, req)
			return maWSReply{Result: `{"authenticated":true}`}
		case "player_queues/get_active_queue":
			return maWSReply{Result: fmt.Sprintf(`{"queue_id":"group123","state":"playing","elapsed_time":12.3,"elapsed_time_last_updated":%.6f}`, float64(updatedAt.UnixNano())/float64(time.Second))}
		case "player_queues/get":
			if req.Args["queue_id"] != "group123" {
				t.Fatalf("unexpected queue_id: %v", req.Args["queue_id"])
			}
			return maWSReply{Result: fmt.Sprintf(`{"queue_id":"group123","state":"playing","elapsed_time":12.3,"elapsed_time_last_updated":%.6f}`, float64(updatedAt.UnixNano())/float64(time.Second))}
		default:
			t.Fatalf("unexpected command: %s", req.Command)
			return maWSReply{}
		}
	})
	defer server.Close()

	adapter := directAdapter(&cfg, server.URL)
	status, err := adapter.GetStatus()
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if status.State != "playing" || status.QueueID != "group123" {
		t.Fatalf("unexpected status: %#v", status)
	}
	if !status.HasElapsed || status.Elapsed != 12300*time.Millisecond {
		t.Fatalf("unexpected elapsed: %s has=%v", status.Elapsed, status.HasElapsed)
	}
	pos, ok, err := adapter.PlaybackPosition()
	if err != nil {
		t.Fatalf("PlaybackPosition failed: %v", err)
	}
	if !ok || pos < 14*time.Second || pos > 17*time.Second {
		t.Fatalf("expected corrected playing position around 14s, got %s ok=%v", pos, ok)
	}
}

func TestDirectGetStatusExtractsCurrentURI(t *testing.T) {
	cfg := directCfg()
	sourceURL := "http://source.local/path/song.flac?token=source"
	server := newMAWSServer(t, func(req maWSCommand) maWSReply {
		switch req.Command {
		case "auth":
			expectAuth(t, req)
			return maWSReply{Result: `{"authenticated":true}`}
		case "player_queues/get_active_queue":
			return maWSReply{Result: `{"queue_id":"group123","state":"playing","elapsed_time":0,"current_item":{"media_item":{"item_id":"builtin-id","uri":"builtin://track/canonical","provider_mappings":[{"item_id":"builtin-id","url":"` + sourceURL + `"}]}}}`}
		default:
			t.Fatalf("unexpected command: %s", req.Command)
			return maWSReply{}
		}
	})
	defer server.Close()

	adapter := directAdapter(&cfg, server.URL)
	status, err := adapter.GetStatus()
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if status.CurrentURI != sourceURL {
		t.Fatalf("expected current URI %q, got %q", sourceURL, status.CurrentURI)
	}
	if len(status.CurrentURIs) != 3 || status.CurrentURIs[0] != sourceURL || status.CurrentURIs[1] != "builtin://track/canonical" || status.CurrentURIs[2] != "builtin-id" {
		t.Fatalf("unexpected current URI candidates: %#v", status.CurrentURIs)
	}
}

func TestDirectGetStatusFallbacksToQueueGetForCurrentURI(t *testing.T) {
	cfg := directCfg()
	sourceURL := "http://source.local/path/song.flac?token=source"
	server := newMAWSServer(t, func(req maWSCommand) maWSReply {
		switch req.Command {
		case "auth":
			expectAuth(t, req)
			return maWSReply{Result: `{"authenticated":true}`}
		case "player_queues/get_active_queue":
			return maWSReply{Result: `{"queue_id":"group123","state":"playing","elapsed_time":0}`}
		case "player_queues/get":
			if req.Args["queue_id"] != "group123" {
				t.Fatalf("unexpected queue_id: %v", req.Args["queue_id"])
			}
			return maWSReply{Result: `{"queue_id":"group123","state":"playing","elapsed_time":0,"current_item":{"media_item":{"item_id":"builtin-id","provider_mappings":[{"item_id":"builtin-id","url":"` + sourceURL + `"}]}}}`}
		default:
			t.Fatalf("unexpected command: %s", req.Command)
			return maWSReply{}
		}
	})
	defer server.Close()

	adapter := directAdapter(&cfg, server.URL)
	status, err := adapter.GetStatus()
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if status.CurrentURI != sourceURL {
		t.Fatalf("expected fallback current URI %q, got %q", sourceURL, status.CurrentURI)
	}
}

func TestDirectPlaybackPositionDoesNotAddDeltaWhenPaused(t *testing.T) {
	cfg := directCfg()
	updatedAt := time.Now().Add(-2 * time.Second)
	server := newMAWSServer(t, func(req maWSCommand) maWSReply {
		switch req.Command {
		case "auth":
			expectAuth(t, req)
			return maWSReply{Result: `{"authenticated":true}`}
		case "player_queues/get_active_queue":
			return maWSReply{Result: fmt.Sprintf(`{"queue_id":"group123","state":"paused","elapsed_time":12,"elapsed_time_last_updated":%.6f}`, float64(updatedAt.UnixNano())/float64(time.Second))}
		default:
			t.Fatalf("unexpected command: %s", req.Command)
			return maWSReply{}
		}
	})
	defer server.Close()

	adapter := directAdapter(&cfg, server.URL)
	pos, ok, err := adapter.PlaybackPosition()
	if err != nil {
		t.Fatalf("PlaybackPosition failed: %v", err)
	}
	if !ok || pos != 12*time.Second {
		t.Fatalf("expected paused position 12s, got %s ok=%v", pos, ok)
	}
}

func TestDirectGetStateFallbackToQueueGet(t *testing.T) {
	cfg := directCfg()
	server := newMAWSServer(t, func(req maWSCommand) maWSReply {
		switch req.Command {
		case "auth":
			expectAuth(t, req)
			return maWSReply{Result: `{"authenticated":true}`}
		case "player_queues/get_active_queue":
			return maWSReply{Result: `null`}
		case "player_queues/get":
			if req.Args["queue_id"] != "player123" {
				t.Fatalf("unexpected queue_id: %v", req.Args["queue_id"])
			}
			return maWSReply{Result: `{"queue_id":"player123","state":"paused","elapsed_time":8}`, Close: true}
		default:
			t.Fatalf("unexpected command: %s", req.Command)
			return maWSReply{}
		}
	})
	defer server.Close()

	adapter := directAdapter(&cfg, server.URL)
	state, err := adapter.GetState()
	if err != nil {
		t.Fatalf("GetState failed: %v", err)
	}
	if state != "paused" {
		t.Fatalf("expected paused, got %q", state)
	}
}

func TestDirectWebSocketErrorPayload(t *testing.T) {
	cfg := directCfg()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, rw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Fatalf("hijack websocket: %v", err)
		}
		defer conn.Close()
		_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\n")
		_, _ = rw.WriteString("Upgrade: websocket\r\n")
		_, _ = rw.WriteString("Connection: Upgrade\r\n")
		_, _ = rw.WriteString("Sec-WebSocket-Accept: " + websocketAccept(r.Header.Get("Sec-WebSocket-Key")) + "\r\n\r\n")
		_ = rw.Flush()
		writeServerText(t, rw, `{"server_id":"test"}`)
		msg, _ := readClientText(rw.Reader)
		var req maWSCommand
		_ = json.Unmarshal([]byte(msg), &req)
		writeServerText(t, rw, `{"message_id":"`+req.MessageID+`","result":{"authenticated":true}}`)
		msg, _ = readClientText(rw.Reader)
		_ = json.Unmarshal([]byte(msg), &req)
		writeServerText(t, rw, `{"message_id":"`+req.MessageID+`","error_code":500,"details":"boom"}`)
	}))
	defer server.Close()

	adapter := directAdapter(&cfg, server.URL)
	if err := adapter.Stop(); err == nil {
		t.Fatal("expected websocket error")
	}
}

func TestMusicAssistantWSURLDoesNotDoubleAppendWS(t *testing.T) {
	tests := map[string]string{
		"http://ma.local:8095":  "ws://ma.local:8095/ws",
		"https://ma.local":      "wss://ma.local/ws",
		"ws://ma.local:8095/ws": "ws://ma.local:8095/ws",
		"wss://ma.local/base":   "wss://ma.local/base/ws",
	}
	for raw, want := range tests {
		u, err := musicAssistantWSURL(raw)
		if err != nil {
			t.Fatalf("musicAssistantWSURL(%q): %v", raw, err)
		}
		if u.String() != want {
			t.Fatalf("musicAssistantWSURL(%q)=%q want %q", raw, u.String(), want)
		}
	}
}

func TestRedactMAHidesAuthTokenAndURLQueries(t *testing.T) {
	payload := `{"command":"auth","args":{"token":"secret-token","media":{"item_id":"http://source.local/song.mp3?signature=abc","provider_mappings":[{"url":"http://source.local/song.mp3?token=abc"}]}}}`
	redacted := redactMA(payload)
	if strings.Contains(redacted, "secret-token") || strings.Contains(redacted, "signature=abc") || strings.Contains(redacted, "token=abc") {
		t.Fatalf("redaction leaked sensitive data: %s", redacted)
	}
	if !strings.Contains(redacted, `"token":"***"`) {
		t.Fatalf("redaction should preserve token key with masked value: %s", redacted)
	}
}

func TestDirectTarget(t *testing.T) {
	cfg := directCfg()
	adapter := directAdapter(&cfg, "http://ma.local:8098")
	if adapter.Target() != "player123" {
		t.Errorf("expected Target 'player123', got %q", adapter.Target())
	}
}

var _ = (*bufio.Reader)(nil)
