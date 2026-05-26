package maadapter

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/leko/ma-dlna/internal/config"
)

// TestPlayMedia_MA_Primary_NoMediaTypeForMIME verifies that MA primary
// play_media omits media_type when contentType is a MIME like "audio/flac".
func TestPlayMedia_MA_Primary_NoMediaTypeForMIME(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.MAAdapter.PlayService = "music_assistant.play_media"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/services/music_assistant/play_media" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		// Must not include media_type for MIME types
		if _, exists := payload["media_type"]; exists {
			t.Error("media_type must not be present for MIME content type audio/flac")
		}
		if payload["media_id"] != "http://bridge:8787/live/test.flac?token=abc" {
			t.Errorf("unexpected media_id: %v", payload["media_id"])
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"success": true}]`))
	}))
	defer server.Close()

	cfg.HA.URL = server.URL
	cfg.HA.Token = "test-token"
	cfg.HA.TargetEntityID = "media_player.test"
	adapter := New(&cfg)

	err := adapter.PlayMedia("media_player.test", "http://bridge:8787/live/test.flac?token=abc", "audio/flac")
	if err != nil {
		t.Fatalf("PlayMedia failed: %v", err)
	}
}

// TestPlayMedia_MA_Primary_PreservesValidMAType verifies that MA-native types
// like "track" are still sent as media_type.
func TestPlayMedia_MA_Primary_PreservesValidMAType(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.MAAdapter.PlayService = "music_assistant.play_media"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/services/music_assistant/play_media" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		if mt, exists := payload["media_type"]; !exists || mt != "track" {
			t.Errorf("media_type must be 'track', got %v", payload["media_type"])
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"success": true}]`))
	}))
	defer server.Close()

	cfg.HA.URL = server.URL
	cfg.HA.Token = "test-token"
	cfg.HA.TargetEntityID = "media_player.test"
	adapter := New(&cfg)

	err := adapter.PlayMedia("media_player.test", "http://bridge:8787/live/test.flac?token=abc", "track")
	if err != nil {
		t.Fatalf("PlayMedia failed: %v", err)
	}
}

// TestPlayMedia_Fallback_ConvertsMIMEToMusic verifies that the fallback
// media_player.play_media converts audio/flac to media_content_type "music".
func TestPlayMedia_Fallback_ConvertsMIMEToMusic(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.MAAdapter.PlayService = ""
	cfg.MAAdapter.FallbackPlayService = "media_player.play_media"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/services/media_player/play_media" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)
		if mct, exists := payload["media_content_type"]; !exists || mct != "music" {
			t.Errorf("media_content_type must be 'music' for MIME input, got %v", payload["media_content_type"])
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"success": true}]`))
	}))
	defer server.Close()

	cfg.HA.URL = server.URL
	cfg.HA.Token = "test-token"
	cfg.HA.TargetEntityID = "media_player.test"
	adapter := New(&cfg)

	err := adapter.PlayMedia("media_player.test", "http://bridge:8787/live/test.flac?token=abc", "audio/flac")
	if err != nil {
		t.Fatalf("PlayMedia failed: %v", err)
	}
}

// TestPlayMedia_HANativeRetryForMIME verifies that when MA primary fails on a MIME
// type, the HA-native retry uses media_content_type "music".
func TestPlayMedia_HANativeRetryForMIME(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.MAAdapter.PlayService = "music_assistant.play_media"

	var callCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := callCount.Add(1)
		var payload map[string]any
		json.NewDecoder(r.Body).Decode(&payload)

		switch count {
		case 1:
			// First call: MA-style, no media_type
			if _, exists := payload["media_type"]; exists {
				t.Error("first call must not include media_type for MIME")
			}
			w.WriteHeader(http.StatusInternalServerError) // force retry
		case 2:
			// Retry: HA-native, media_content_type must be "music"
			if mct, exists := payload["media_content_type"]; !exists || mct != "music" {
				t.Errorf("retry media_content_type must be 'music', got %v", payload["media_content_type"])
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`[{"success": true}]`))
		default:
			t.Errorf("unexpected call count: %d", count)
		}
	}))
	defer server.Close()

	cfg.HA.URL = server.URL
	cfg.HA.Token = "test-token"
	cfg.HA.TargetEntityID = "media_player.test"
	adapter := New(&cfg)

	err := adapter.PlayMedia("media_player.test", "http://bridge:8787/live/test.flac?token=abc", "audio/flac")
	if err != nil {
		t.Fatalf("PlayMedia failed: %v", err)
	}
	if got := callCount.Load(); got != 2 {
		t.Errorf("expected 2 calls (MA fail + HA-native retry success), got %d", got)
	}
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
		// Check either MA-style or HA-native field names
		cid := payload["media_id"]
		if cid == nil {
			cid = payload["media_content_id"]
		}
		if cid != "http://bridge:8787/live/test.opus?token=abc" {
			t.Errorf("unexpected media id: %v", cid)
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
		if r.URL.Path != "/api/services/media_player/media_stop" {
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
		if r.URL.Path != "/api/services/media_player/media_pause" {
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
		if r.URL.Path != "/api/services/media_player/play_media" {
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
