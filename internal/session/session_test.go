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

func TestCreateStopsLoadedSessions(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	// Create two loaded sessions — second Create should stop the first
	s1 := mgr.Create("http://source.local/first", "")
	if s1.State != StateLoaded {
		t.Fatalf("s1 should be loaded, got %s", s1.State)
	}

	s2 := mgr.Create("http://source.local/second", "")
	if s2.State != StateLoaded {
		t.Fatalf("s2 should be loaded, got %s", s2.State)
	}

	// s1 should be stopped when s2 was created
	if mgr.Get(s1.ID).State != StateStopped {
		t.Errorf("s1 should be stopped after second Create, got %s", mgr.Get(s1.ID).State)
	}

	// ActiveSession should return s2 (the only loaded one)
	active := mgr.ActiveSession()
	if active == nil {
		t.Fatal("ActiveSession should not be nil")
	}
	if active.ID != s2.ID {
		t.Errorf("ActiveSession should return s2 (%s), got %s", s2.ID, active.ID)
	}
}

func TestMultipleSetAVTransportURIPlaysCorrectSession(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	// Simulate: controller sends SetAVTransportURI twice, then Play
	s1 := mgr.Create("http://source.local/first", "")
	s2 := mgr.Create("http://source.local/second", "")

	// s1 should be stopped, s2 loaded
	if mgr.Get(s1.ID).State != StateStopped {
		t.Errorf("s1 should be stopped, got %s", mgr.Get(s1.ID).State)
	}

	// Play should find s2 as the active session
	active := mgr.ActiveSession()
	if active == nil || active.ID != s2.ID {
		t.Fatalf("ActiveSession should return s2, got %v", active)
	}

	mgr.Play(active.ID)
	if mgr.Get(s2.ID).State != StateStarting {
		t.Errorf("s2 should be starting, got %s", mgr.Get(s2.ID).State)
	}
	if mgr.Get(s1.ID).State != StateStopped {
		t.Errorf("s1 should remain stopped, got %s", mgr.Get(s1.ID).State)
	}
}

func TestActiveSessionReturnsOnlyValidStates(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	// Create a loaded session, play it, then stop it to get a stopped session
	loaded := mgr.Create("http://source.local/loaded", "")
	mgr.Play(loaded.ID)

	// Create another that replaces the first (first is now stopped, second is loaded)
	second := mgr.Create("http://source.local/second", "")

	// ActiveSession should return the second (loaded), not the first (stopped)
	active := mgr.ActiveSession()
	if active == nil {
		t.Fatal("ActiveSession should return loaded session")
	}
	if active.ID != second.ID {
		t.Errorf("expected second session %s, got %s", second.ID, active.ID)
	}

	// Stop it — no active session
	mgr.Stop(second.ID)
	active = mgr.ActiveSession()
	if active != nil {
		t.Errorf("expected no active session, got %s", active.ID)
	}
}

func TestSetPlayingDoesNotOverrideError(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s := mgr.Create("http://source.local/test", "")
	mgr.SetError(s.ID, "some error")

	// SetPlaying should NOT override the error state
	mgr.SetPlaying(s.ID)
	got := mgr.Get(s.ID)
	if got.State != StateError {
		t.Errorf("SetPlaying should not override error state, got %s", got.State)
	}
	if got.Error != "some error" {
		t.Errorf("error message should be preserved, got %s", got.Error)
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
