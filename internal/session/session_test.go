package session

import (
	"testing"
	"time"

	"github.com/leko/ma-dlna/internal/config"
	"github.com/leko/ma-dlna/internal/stream"
)

func TestStateTransitions(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s := mgr.Create("http://source.local/test.flac", "")
	if s.State != StateLoaded {
		t.Errorf("expected loaded state, got %s", s.State)
	}

	if s.ID == "" {
		t.Error("session ID should not be empty")
	}
	if s.StreamToken == "" {
		t.Error("stream token should not be empty")
	}

	// Play from loaded
	err := mgr.Play(s.ID)
	if err != nil {
		t.Fatalf("play failed: %v", err)
	}
	if mgr.Get(s.ID).State != StateStarting {
		t.Errorf("expected starting state, got %s", mgr.Get(s.ID).State)
	}

	// SetPlaying
	mgr.SetPlaying(s.ID)
	if mgr.Get(s.ID).State != StatePlaying {
		t.Errorf("expected playing state, got %s", mgr.Get(s.ID).State)
	}

	// Pause
	err = mgr.Pause(s.ID)
	if err != nil {
		t.Fatalf("pause failed: %v", err)
	}
	if mgr.Get(s.ID).State != StatePaused {
		t.Errorf("expected paused state, got %s", mgr.Get(s.ID).State)
	}

	// Play from paused — resumes with ffmpeg -ss offset, goes to starting
	err = mgr.Play(s.ID)
	if err != nil {
		t.Fatalf("play from paused failed: %v", err)
	}
	if mgr.Get(s.ID).State != StateStarting {
		t.Errorf("expected starting state after play from paused, got %s", mgr.Get(s.ID).State)
	}

	// Stop
	err = mgr.Stop(s.ID)
	if err != nil {
		t.Fatalf("stop failed: %v", err)
	}
	if mgr.Get(s.ID).State != StateStopped {
		t.Errorf("expected stopped state, got %s", mgr.Get(s.ID).State)
	}
}

func TestCreateSessionReplacesActive(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s1 := mgr.Create("http://source.local/1", "")
	mgr.Play(s1.ID)
	mgr.SetPlaying(s1.ID)

	if mgr.Get(s1.ID).State != StatePlaying {
		t.Error("first session should be playing")
	}

	s2 := mgr.Create("http://source.local/2", "")

	if mgr.Get(s1.ID).State != StateStopped {
		t.Errorf("first session should be stopped, got %s", mgr.Get(s1.ID).State)
	}
	if s2.State != StateLoaded {
		t.Errorf("second session should be loaded, got %s", s2.State)
	}
}

func TestTokenValidation(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Security.RequireStreamToken = true
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s := mgr.Create("http://source.local/test.mp3", "")

	if mgr.ValidateToken(s.ID, s.StreamToken) != true {
		t.Error("valid token should pass")
	}
	if mgr.ValidateToken(s.ID, "wrong-token") != false {
		t.Error("wrong token should fail")
	}
	if mgr.ValidateToken("nonexistent", "any-token") != false {
		t.Error("unknown session should fail")
	}

	// With require_token disabled
	cfg.Security.RequireStreamToken = false
	if mgr.ValidateToken(s.ID, "any-token") != true {
		t.Error("any token should pass when require_token is disabled")
	}
}

func TestMetadataParsing(t *testing.T) {
	md := parseDIDL("")
	if md == nil {
		t.Error("empty metadata should return empty struct, not nil")
	}

	md = parseDIDL(`<item><title>Test Song</title><creator>Artist Name</creator><album>Test Album</album></item>`)
	if md.Title != "Test Song" {
		t.Errorf("expected 'Test Song', got '%s'", md.Title)
	}
	if md.Artist != "Artist Name" {
		t.Errorf("expected 'Artist Name', got '%s'", md.Artist)
	}
	if md.Album != "Test Album" {
		t.Errorf("expected 'Test Album', got '%s'", md.Album)
	}

	// With DIDL-Lite wrapper (as real controllers send)
	md = parseDIDL(`<DIDL-Lite><item><title>Wrapped</title><artist>Wrapper</artist></item></DIDL-Lite>`)
	if md.Title != "Wrapped" {
		t.Errorf("DIDL-Lite wrapped: expected 'Wrapped', got '%s'", md.Title)
	}
	if md.Artist != "Wrapper" {
		t.Errorf("DIDL-Lite wrapped: expected 'Wrapper', got '%s'", md.Artist)
	}
}

func TestStreamURLGeneration(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Server.PublicBaseURL = "http://bridge.local:8787"
	cfg.FFmpeg.OutputFormat = "opus"
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s := mgr.Create("http://source.local/test", "")

	expected := "http://bridge.local:8787/live/" + s.ID + ".opus?token=" + s.StreamToken
	if s.StreamURL != expected {
		t.Errorf("expected URL '%s', got '%s'", expected, s.StreamURL)
	}
}

func TestSessionTimestamps(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	before := time.Now()
	s := mgr.Create("http://source.local/test", "")
	after := time.Now()

	if s.CreatedAt.Before(before) || s.CreatedAt.After(after) {
		t.Error("created_at should be between before and after")
	}
}

func TestNotFound(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	if err := mgr.Play("nonexistent"); err != ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
	if s := mgr.Get("nonexistent"); s != nil {
		t.Error("expected nil for unknown session")
	}
}

func TestCountAndAllSessions(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	if mgr.Count() != 0 {
		t.Error("initial count should be 0")
	}

	mgr.Create("http://source.local/1", "")
	mgr.Create("http://source.local/2", "")

	if mgr.Count() != 2 {
		t.Errorf("expected count 2, got %d", mgr.Count())
	}
	if len(mgr.AllSessions()) != 2 {
		t.Error("expected 2 sessions in list")
	}
}

func TestSessionEviction(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	// Fill past maxSessions with stopped sessions — triggers eviction
	for i := 0; i < maxSessions+5; i++ {
		s := mgr.Create("http://source.local/test", "")
		mgr.Stop(s.ID)
	}

	if mgr.Count() > maxSessions {
		t.Errorf("should not exceed maxSessions, got %d", mgr.Count())
	}
}

func TestSetError(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s := mgr.Create("http://source.local/test", "")
	mgr.SetError(s.ID, "test error")

	got := mgr.Get(s.ID)
	if got.State != StateError {
		t.Errorf("expected error state, got %s", got.State)
	}
	if got.Error != "test error" {
		t.Errorf("expected error message, got %s", got.Error)
	}
}
