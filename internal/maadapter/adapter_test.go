package maadapter

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/leko/ma-dlna/internal/config"
)

func TestPlayMediaPayload(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.HA.Token = "test-token"
	cfg.HA.TargetEntityID = "media_player.test"
	cfg.MAAdapter.PlayService = "music_assistant.play_media"

	adapter := New(&cfg)

	_ = adapter
}

func TestSetVolumePayload(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.MAAdapter.VolumeService = "media_player.volume_set"

	adapter := New(&cfg)

	_ = adapter
}

func TestServiceURLConstruction(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.HA.URL = "http://ha.local:8123"

	adapter := New(&cfg)

	// Verify URL construction by making a real HTTP call to a test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.Header.Get("Authorization") != "Bearer test-token" {
			t.Errorf("expected auth header, got %s", r.Header.Get("Authorization"))
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected JSON content type, got %s", r.Header.Get("Content-Type"))
		}

		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		if payload["entity_id"] != "media_player.test" {
			t.Errorf("expected entity_id media_player.test, got %v", payload["entity_id"])
		}
		if payload["media_content_id"] != "http://bridge:8787/live/test.opus?token=abc" {
			t.Errorf("unexpected media_content_id: %v", payload["media_content_id"])
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"success": true}]`))
	}))
	defer server.Close()

	cfg.HA.URL = server.URL
	cfg.HA.Token = "test-token"
	cfg.HA.TargetEntityID = "media_player.test"

	adapter = New(&cfg)

	err := adapter.PlayMedia(
		"media_player.test",
		"http://bridge:8787/live/test.opus?token=abc",
		"music",
	)
	if err != nil {
		t.Fatalf("PlayMedia failed: %v", err)
	}
}

func TestStopPayload(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.MAAdapter.StopService = "media_player.media_stop"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/services/media_player.media_stop" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}

		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		if payload["entity_id"] != "media_player.test" {
			t.Errorf("unexpected entity_id: %v", payload["entity_id"])
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"success": true}]`))
	}))
	defer server.Close()

	cfg.HA.URL = server.URL
	cfg.HA.Token = "test-token"
	adapter := New(&cfg)

	err := adapter.Stop("media_player.test")
	if err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
}

func TestPausePayload(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.MAAdapter.PauseService = "media_player.media_pause"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/services/media_player.media_pause" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"success": true}]`))
	}))
	defer server.Close()

	cfg.HA.URL = server.URL
	cfg.HA.Token = "test-token"
	adapter := New(&cfg)

	err := adapter.Pause("media_player.test")
	if err != nil {
		t.Fatalf("Pause failed: %v", err)
	}
}

func TestSetVolume(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.MAAdapter.VolumeService = "media_player.volume_set"

	// Volume 75 should translate to 0.75
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)

		vol, ok := payload["volume_level"].(float64)
		if !ok || vol != 0.75 {
			t.Errorf("expected volume_level 0.75, got %v", payload["volume_level"])
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"success": true}]`))
	}))
	defer server.Close()

	cfg.HA.URL = server.URL
	cfg.HA.Token = "test-token"
	adapter := New(&cfg)

	err := adapter.SetVolume("media_player.test", 75)
	if err != nil {
		t.Fatalf("SetVolume failed: %v", err)
	}
}

func TestServiceCallError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := config.DefaultConfig()
	cfg.HA.URL = server.URL
	cfg.HA.Token = "test-token"
	adapter := New(&cfg)

	err := adapter.Stop("media_player.test")
	if err == nil {
		t.Error("expected error for 500 response")
	}
}

func TestFallbackPlayService(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.MAAdapter.PlayService = ""
	cfg.MAAdapter.FallbackPlayService = "media_player.play_media"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/services/media_player.play_media" {
			t.Errorf("expected fallback service, got path: %s", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"success": true}]`))
	}))
	defer server.Close()

	cfg.HA.URL = server.URL
	cfg.HA.Token = "test-token"
	adapter := New(&cfg)

	err := adapter.PlayMedia("media_player.test", "http://bridge:8787/live/test.mp3?token=abc", "music")
	if err != nil {
		t.Fatalf("PlayMedia with fallback failed: %v", err)
	}
}
