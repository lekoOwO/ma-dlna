package maadapter

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/leko/ma-dlna/internal/config"
)

func directCfg() config.Config {
	cfg := config.DefaultConfig()
	cfg.MAAdapter.Mode = "direct"
	cfg.MusicAssistant.URL = ""
	cfg.MusicAssistant.Token = "ma-token"
	cfg.MusicAssistant.TargetPlayerID = "player123"
	return cfg
}

func directAdapter(cfg *config.Config, srvURL string) *DirectAdapter {
	cfg.MusicAssistant.URL = srvURL
	return newDirectAdapter(cfg)
}

func TestDirectPlayMediaPayload(t *testing.T) {
	cfg := directCfg()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Authorization") != "Bearer ma-token" {
			t.Errorf("expected auth header, got %s", r.Header.Get("Authorization"))
		}

		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)

		switch body["command"] {
		case "player_queues/get_active_queue":
			w.Write([]byte(`{"result":{"queue_id":"group123","state":"idle"}}`))
			return
		case "player_queues/play_media":
		default:
			t.Errorf("unexpected command: %v", body["command"])
		}

		args, _ := body["args"].(map[string]any)
		if args["queue_id"] != "group123" {
			t.Errorf("unexpected queue_id: %v", args["queue_id"])
		}
		if args["option"] != "replace" {
			t.Errorf("unexpected option: %v", args["option"])
		}

		media, _ := args["media"].(map[string]any)
		if media["item_id"] != "http://bridge:8787/live/test.opus?token=abc" {
			t.Errorf("unexpected item_id: %v", media["item_id"])
		}
		if media["provider"] != "builtin" {
			t.Errorf("unexpected provider: %v", media["provider"])
		}
		if media["name"] != "Test Song" {
			t.Errorf("unexpected name: %v", media["name"])
		}
		if media["media_type"] != "track" {
			t.Errorf("unexpected media_type: %v", media["media_type"])
		}

		// Check duration
		if media["duration"].(float64) != 246 {
			t.Errorf("unexpected duration: %v", media["duration"])
		}

		// Check artists
		artists := media["artists"].([]any)
		if len(artists) != 1 {
			t.Fatalf("expected 1 artist, got %d", len(artists))
		}
		artist := artists[0].(map[string]any)
		if artist["name"] != "Test Artist" {
			t.Errorf("unexpected artist name: %v", artist["name"])
		}
		if _, ok := artist["provider_mappings"].([]any); !ok {
			t.Errorf("artist provider_mappings missing or invalid: %v", artist["provider_mappings"])
		}

		album := media["album"].(map[string]any)
		if album["name"] != "Test Album" {
			t.Errorf("unexpected album name: %v", album["name"])
		}
		if _, ok := album["provider_mappings"].([]any); !ok {
			t.Errorf("album provider_mappings missing or invalid: %v", album["provider_mappings"])
		}

		metadata := media["metadata"].(map[string]any)
		images := metadata["images"].([]any)
		image := images[0].(map[string]any)
		if image["path"] != "http://images.local/cover.jpg" {
			t.Errorf("unexpected album art path: %v", image["path"])
		}

		// Check provider_mappings
		pms := media["provider_mappings"].([]any)
		if len(pms) != 1 {
			t.Fatalf("expected 1 provider_mapping, got %d", len(pms))
		}
		pm := pms[0].(map[string]any)
		af, _ := pm["audio_format"].(map[string]any)
		if af["content_type"] != "ogg" {
			t.Errorf("expected audio format ogg, got %v", af["content_type"])
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
	defer server.Close()

	adapter := directAdapter(&cfg, server.URL)

	err := adapter.PlayMedia(PlayRequest{
		StreamURL:   "http://bridge:8787/live/test.opus?token=abc",
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
}

func TestDirectPlayMediaUsesSourceURLWhenPresent(t *testing.T) {
	cfg := directCfg()
	sourceURL := "http://source.local/path/song.mp3?token=source"
	streamURL := "http://bridge:8787/live/test.ogg?token=bridge"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)

		switch body["command"] {
		case "player_queues/get_active_queue":
			w.Write([]byte(`{"result":{"queue_id":"group123","state":"idle"}}`))
			return
		case "player_queues/play_media":
		default:
			t.Errorf("unexpected command: %v", body["command"])
		}

		args, _ := body["args"].(map[string]any)
		media, _ := args["media"].(map[string]any)
		if media["item_id"] != sourceURL {
			t.Fatalf("direct mode should send source URL as item_id, got %v", media["item_id"])
		}

		pms := media["provider_mappings"].([]any)
		pm := pms[0].(map[string]any)
		if pm["item_id"] != sourceURL {
			t.Fatalf("direct mode provider mapping should use source URL, got %v", pm["item_id"])
		}
		if pm["url"] != sourceURL {
			t.Fatalf("direct mode provider mapping url should use source URL, got %v", pm["url"])
		}
		if media["item_id"] == streamURL || pm["item_id"] == streamURL {
			t.Fatal("direct mode must not send bridge stream URL to MA when source URL is available")
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
	defer server.Close()

	adapter := directAdapter(&cfg, server.URL)

	err := adapter.PlayMedia(PlayRequest{
		StreamURL:   streamURL,
		SourceURL:   sourceURL,
		ContentType: "audio/mpeg",
		Title:       "Test Song",
	})
	if err != nil {
		t.Fatalf("PlayMedia failed: %v", err)
	}
}

func TestDirectPlayMedia_NoToken(t *testing.T) {
	cfg := directCfg()
	cfg.MusicAssistant.Token = ""

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Error("Authorization header should be absent when token is empty")
		}
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["command"] == "player_queues/get_active_queue" {
			w.Write([]byte(`{"result":null}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
	defer server.Close()

	adapter := directAdapter(&cfg, server.URL)

	err := adapter.PlayMedia(PlayRequest{
		StreamURL:   "http://bridge:8787/live/test.opus",
		ContentType: "audio/ogg",
	})
	if err != nil {
		t.Fatalf("PlayMedia failed: %v", err)
	}
}

func TestDirectPlayMedia_NameFallback(t *testing.T) {
	cfg := directCfg()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["command"] == "player_queues/get_active_queue" {
			w.Write([]byte(`{"result":null}`))
			return
		}
		args, _ := body["args"].(map[string]any)
		media, _ := args["media"].(map[string]any)
		if media["name"] != "song.flac" {
			t.Errorf("name should fallback to source URL basename, got %v", media["name"])
		}
		// Artists should be absent
		if _, exists := media["artists"]; exists {
			t.Error("artists should not be present when artist is empty")
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
	defer server.Close()

	adapter := directAdapter(&cfg, server.URL)

	err := adapter.PlayMedia(PlayRequest{
		StreamURL:   "http://bridge:8787/live/test.opus",
		SourceURL:   "http://source.local/path/song.flac?token=source",
		ContentType: "audio/ogg",
	})
	if err != nil {
		t.Fatalf("PlayMedia failed: %v", err)
	}
}

func TestDirectPlayMedia_AudioFormatMapping(t *testing.T) {
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
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				json.NewDecoder(r.Body).Decode(&body)
				if body["command"] == "player_queues/get_active_queue" {
					w.Write([]byte(`{"result":{"queue_id":"group123","state":"idle"}}`))
					return
				}
				args, _ := body["args"].(map[string]any)
				if args["queue_id"] != "group123" {
					t.Errorf("unexpected queue_id: %v", args["queue_id"])
				}
				media, _ := args["media"].(map[string]any)
				pms := media["provider_mappings"].([]any)
				pm := pms[0].(map[string]any)
				af, _ := pm["audio_format"].(map[string]any)
				if af["content_type"] != tc.expectedFmt {
					t.Errorf("expected %q, got %q", tc.expectedFmt, af["content_type"])
				}
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{}`))
			}))
			defer server.Close()

			adapter := directAdapter(&cfg, server.URL)
			err := adapter.PlayMedia(PlayRequest{
				StreamURL:   "http://bridge:8787/live/test",
				ContentType: tc.mime,
			})
			if err != nil {
				t.Fatalf("PlayMedia failed: %v", err)
			}
		})
	}
}

func TestDirectStop(t *testing.T) {
	cfg := directCfg()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		switch body["command"] {
		case "player_queues/get_active_queue":
			w.Write([]byte(`{"result":{"queue_id":"group123","state":"playing"}}`))
			return
		case "player_queues/stop":
		default:
			t.Errorf("unexpected command: %v", body["command"])
		}
		args, _ := body["args"].(map[string]any)
		if args["queue_id"] != "group123" {
			t.Errorf("unexpected queue_id: %v", args["queue_id"])
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
	defer server.Close()

	adapter := directAdapter(&cfg, server.URL)
	if err := adapter.Stop(); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
}

func TestDirectPause(t *testing.T) {
	cfg := directCfg()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		switch body["command"] {
		case "player_queues/get_active_queue":
			w.Write([]byte(`{"result":{"queue_id":"group123","state":"playing"}}`))
			return
		case "player_queues/pause":
		default:
			t.Errorf("unexpected command: %v", body["command"])
		}
		args, _ := body["args"].(map[string]any)
		if args["queue_id"] != "group123" {
			t.Errorf("unexpected queue_id: %v", args["queue_id"])
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
	defer server.Close()

	adapter := directAdapter(&cfg, server.URL)
	if err := adapter.Pause(); err != nil {
		t.Fatalf("Pause failed: %v", err)
	}
}

func TestDirectResume(t *testing.T) {
	cfg := directCfg()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		switch body["command"] {
		case "player_queues/get_active_queue":
			w.Write([]byte(`{"result":{"queue_id":"group123","state":"paused"}}`))
			return
		case "player_queues/play":
		default:
			t.Errorf("unexpected command: %v", body["command"])
		}
		args, _ := body["args"].(map[string]any)
		if args["queue_id"] != "group123" {
			t.Errorf("unexpected queue_id: %v", args["queue_id"])
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
	defer server.Close()

	adapter := directAdapter(&cfg, server.URL)
	if err := adapter.Resume(); err != nil {
		t.Fatalf("Resume failed: %v", err)
	}
}

func TestDirectSeek(t *testing.T) {
	cfg := directCfg()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		switch body["command"] {
		case "player_queues/get_active_queue":
			w.Write([]byte(`{"result":{"queue_id":"group123","state":"playing"}}`))
			return
		case "player_queues/seek":
		default:
			t.Errorf("unexpected command: %v", body["command"])
		}
		args, _ := body["args"].(map[string]any)
		if args["queue_id"] != "group123" {
			t.Errorf("unexpected queue_id: %v", args["queue_id"])
		}
		if args["position"].(float64) != 42 {
			t.Errorf("unexpected seek position: %v", args["position"])
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
	defer server.Close()

	adapter := directAdapter(&cfg, server.URL)
	if err := adapter.Seek(42 * time.Second); err != nil {
		t.Fatalf("Seek failed: %v", err)
	}
}

func TestDirectSetVolume(t *testing.T) {
	cfg := directCfg()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["command"] != "players/cmd/volume_set" {
			t.Errorf("unexpected command: %v", body["command"])
		}
		args, _ := body["args"].(map[string]any)
		if args["player_id"] != "player123" {
			t.Errorf("unexpected player_id: %v", args["player_id"])
		}
		if args["volume_level"].(float64) != 75 {
			t.Errorf("unexpected volume_level: %v", args["volume_level"])
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
	}))
	defer server.Close()

	adapter := directAdapter(&cfg, server.URL)
	if err := adapter.SetVolume(75); err != nil {
		t.Fatalf("SetVolume failed: %v", err)
	}
}

func TestDirectGetState_ActiveQueue(t *testing.T) {
	cfg := directCfg()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)

		switch body["command"] {
		case "player_queues/get_active_queue":
			args, _ := body["args"].(map[string]any)
			if args["player_id"] != "player123" {
				t.Errorf("unexpected player_id: %v", args["player_id"])
			}
			w.Write([]byte(`{"result":{"queue_id":"group123","state":"playing","elapsed_time":12.3}}`))
		default:
			t.Errorf("unexpected command: %v", body["command"])
		}
	}))
	defer server.Close()

	adapter := directAdapter(&cfg, server.URL)
	status, err := adapter.GetStatus()
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if status.State != "playing" {
		t.Errorf("expected 'playing', got %q", status.State)
	}
	if status.QueueID != "group123" {
		t.Errorf("expected queue group123, got %q", status.QueueID)
	}
	if !status.HasElapsed || status.Elapsed != 12300*time.Millisecond {
		t.Errorf("expected elapsed 12.3s, got %s has=%v", status.Elapsed, status.HasElapsed)
	}
	pos, ok, err := adapter.PlaybackPosition()
	if err != nil {
		t.Fatalf("PlaybackPosition failed: %v", err)
	}
	if !ok || pos != 12300*time.Millisecond {
		t.Errorf("expected playback position 12.3s, got %s ok=%v", pos, ok)
	}
}

func TestDirectPlaybackPositionAddsPlayingDelta(t *testing.T) {
	cfg := directCfg()
	updatedAt := time.Now().Add(-2 * time.Second)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["command"] != "player_queues/get_active_queue" {
			t.Errorf("unexpected command: %v", body["command"])
		}
		w.Write([]byte(fmt.Sprintf(
			`{"result":{"queue_id":"group123","state":"playing","elapsed_time":12,"elapsed_time_last_updated":%.6f}}`,
			float64(updatedAt.UnixNano())/float64(time.Second),
		)))
	}))
	defer server.Close()

	adapter := directAdapter(&cfg, server.URL)
	pos, ok, err := adapter.PlaybackPosition()
	if err != nil {
		t.Fatalf("PlaybackPosition failed: %v", err)
	}
	if !ok {
		t.Fatal("expected playback position to be available")
	}
	if pos < 14*time.Second || pos > 17*time.Second {
		t.Errorf("expected corrected position around 14s, got %s", pos)
	}
}

func TestDirectPlaybackPositionDoesNotAddDeltaWhenPaused(t *testing.T) {
	cfg := directCfg()
	updatedAt := time.Now().Add(-2 * time.Second)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["command"] != "player_queues/get_active_queue" {
			t.Errorf("unexpected command: %v", body["command"])
		}
		w.Write([]byte(fmt.Sprintf(
			`{"result":{"queue_id":"group123","state":"paused","elapsed_time":12,"elapsed_time_last_updated":%.6f}}`,
			float64(updatedAt.UnixNano())/float64(time.Second),
		)))
	}))
	defer server.Close()

	adapter := directAdapter(&cfg, server.URL)
	pos, ok, err := adapter.PlaybackPosition()
	if err != nil {
		t.Fatalf("PlaybackPosition failed: %v", err)
	}
	if !ok {
		t.Fatal("expected playback position to be available")
	}
	if pos != 12*time.Second {
		t.Errorf("expected paused position to remain 12s, got %s", pos)
	}
}

func TestDirectGetState_FallbackToQueueGet(t *testing.T) {
	cfg := directCfg()

	var callCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)

		switch body["command"] {
		case "player_queues/get_active_queue":
			w.Write([]byte(`{"result":null}`))
		case "player_queues/get":
			args, _ := body["args"].(map[string]any)
			if args["queue_id"] != "player123" {
				t.Errorf("unexpected queue_id: %v", args["queue_id"])
			}
			w.Write([]byte(`{"result":{"queue_id":"player123","state":"paused","elapsed_time":8}}`))
		default:
			t.Errorf("unexpected command: %v", body["command"])
		}
	}))
	defer server.Close()

	adapter := directAdapter(&cfg, server.URL)
	state, err := adapter.GetState()
	if err != nil {
		t.Fatalf("GetState failed: %v", err)
	}
	if state != "paused" {
		t.Errorf("expected 'paused', got %q", state)
	}
	if callCount != 2 {
		t.Errorf("expected 2 calls, got %d", callCount)
	}
}

func TestDirectGetState_NormalizesUnknown(t *testing.T) {
	cfg := directCfg()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		switch body["command"] {
		case "player_queues/get_active_queue":
			w.Write([]byte(`{"result":{"state":"unknown"}}`))
		default:
			t.Errorf("unexpected command: %v", body["command"])
		}
	}))
	defer server.Close()

	adapter := directAdapter(&cfg, server.URL)
	state, err := adapter.GetState()
	if err != nil {
		t.Fatalf("GetState failed: %v", err)
	}
	if state != "unknown" {
		t.Errorf("expected 'unknown', got %q", state)
	}
}

func TestDirectNon2xxError(t *testing.T) {
	cfg := directCfg()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	adapter := directAdapter(&cfg, server.URL)
	err := adapter.Stop()
	if err == nil {
		t.Error("expected error for 500 response")
	}
}

func TestDirectErrorPayload(t *testing.T) {
	cfg := directCfg()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"error": "something went wrong"}`))
	}))
	defer server.Close()

	adapter := directAdapter(&cfg, server.URL)
	err := adapter.Stop()
	if err == nil {
		t.Error("expected error when response contains 'error' field")
	}
}

func TestDirectTarget(t *testing.T) {
	cfg := directCfg()
	adapter := directAdapter(&cfg, "http://ma.local:8098")
	if adapter.Target() != "player123" {
		t.Errorf("expected Target 'player123', got %q", adapter.Target())
	}
}
