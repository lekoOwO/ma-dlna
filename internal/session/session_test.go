package session

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/leko/ma-dlna/internal/config"
	"github.com/leko/ma-dlna/internal/stream"
)

func newManagerWithFakeFFmpeg(t *testing.T) (*Manager, *stream.Streamer) {
	t.Helper()

	cfg := config.DefaultConfig()
	cfg.FFmpeg.Binary = writeFakeFFmpeg(t)
	streamer := stream.NewStreamer(&cfg)

	return NewManager(&cfg, streamer), streamer
}

func writeFakeFFmpeg(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "ffmpeg")
	script := `#!/bin/sh
exec sleep 3600
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake ffmpeg: %v", err)
	}
	return path
}

func waitForStreamRunning(t *testing.T, streamer *stream.Streamer, sessionID string) {
	t.Helper()
	waitForCondition(t, 500*time.Millisecond, "stream to be running", func() bool {
		return streamer.IsRunning(sessionID)
	})
}

func waitForStreamStopped(t *testing.T, streamer *stream.Streamer, sessionID string) {
	t.Helper()
	waitForCondition(t, 500*time.Millisecond, "stream to stop", func() bool {
		return !streamer.IsRunning(sessionID)
	})
}

func waitForCondition(t *testing.T, timeout time.Duration, description string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if condition() {
		return
	}
	t.Fatalf("timed out waiting for %s", description)
}

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

	expected := "http://bridge.local:8787/live/" + s.ID + ".ogg?token=" + s.StreamToken
	if s.StreamURL != expected {
		t.Errorf("expected URL '%s', got '%s'", expected, s.StreamURL)
	}
}

func TestRelativeAlbumArtURIResolvedAgainstSource(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s := mgr.Create(
		"http://source.local/music/song.flac?token=abc",
		`<item><title>Song</title><albumArtURI>covers/song.jpg</albumArtURI></item>`,
	)

	expected := "http://source.local/music/covers/song.jpg"
	if s.Metadata.AlbumArtURI != expected {
		t.Fatalf("expected album art %q, got %q", expected, s.Metadata.AlbumArtURI)
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

func TestStopIfGenerationStopsMatchingGenerationStream(t *testing.T) {
	mgr, streamer := newManagerWithFakeFFmpeg(t)

	s := mgr.Create("http://source.local/test", "")
	var genID uint64
	streamer.SetGenStartCallback(func(sessionID string, gid uint64) {
		mgr.SetSessionGenID(sessionID, gid)
		if sessionID == s.ID {
			genID = gid
		}
	})
	mgr.StartStream(s.ID, s.SourceURI)
	t.Cleanup(func() {
		streamer.Stop(s.ID)
	})

	waitForStreamRunning(t, streamer, s.ID)
	if genID == 0 {
		t.Fatal("expected stream generation to be recorded")
	}

	if !mgr.StopIfGeneration(s.ID, genID) {
		t.Fatal("StopIfGeneration should return true for matching gen")
	}
	if got := mgr.Get(s.ID).State; got != StateStopped {
		t.Errorf("expected stopped state, got %s", got)
	}
	waitForStreamStopped(t, streamer, s.ID)
}

func TestStopIfGenerationRejectsStaleGenAndLeavesStreamRunning(t *testing.T) {
	mgr, streamer := newManagerWithFakeFFmpeg(t)

	s := mgr.Create("http://source.local/test", "")
	mgr.SetSessionGenID(s.ID, 42)
	mgr.StartStream(s.ID, s.SourceURI)
	t.Cleanup(func() {
		streamer.Stop(s.ID)
	})

	waitForStreamRunning(t, streamer, s.ID)

	if mgr.StopIfGeneration(s.ID, 42) {
		t.Fatal("StopIfGeneration should return false when streamer rejects mismatched gen")
	}
	if got := mgr.Get(s.ID).State; got != StateLoaded {
		t.Errorf("state should remain loaded, got %s", got)
	}
	if !streamer.IsRunning(s.ID) {
		t.Error("stream should remain running after stale generation is rejected")
	}
	streamer.Stop(s.ID)
	waitForStreamStopped(t, streamer, s.ID)
}

func TestStopIfGenerationRejectsErrorState(t *testing.T) {
	mgr, streamer := newManagerWithFakeFFmpeg(t)

	s := mgr.Create("http://source.local/test", "")
	mgr.SetSessionGenID(s.ID, 42)
	mgr.StartStream(s.ID, s.SourceURI)
	t.Cleanup(func() {
		streamer.Stop(s.ID)
	})
	waitForStreamRunning(t, streamer, s.ID)

	mgr.SetError(s.ID, "some error")

	if mgr.StopIfGeneration(s.ID, 42) {
		t.Fatal("StopIfGeneration should return false for error state")
	}
	got := mgr.Get(s.ID)
	if got.State != StateError {
		t.Errorf("state should remain error, got %s", got.State)
	}
	if got.Error != "some error" {
		t.Errorf("error message should be preserved, got %s", got.Error)
	}
	if !streamer.IsRunning(s.ID) {
		t.Error("stream should remain running when error state rejects StopIfGeneration")
	}
	streamer.Stop(s.ID)
	waitForStreamStopped(t, streamer, s.ID)
}

func TestStopIfGenerationMissingSessionReturnsFalse(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	if mgr.StopIfGeneration("missing", 42) {
		t.Fatal("StopIfGeneration should return false for a missing session")
	}
}

func TestStopIfGenerationZeroGenSkipsGenerationCheck(t *testing.T) {
	mgr, streamer := newManagerWithFakeFFmpeg(t)

	s := mgr.Create("http://source.local/test", "")
	mgr.SetSessionGenID(s.ID, 42)
	mgr.StartStream(s.ID, s.SourceURI)
	t.Cleanup(func() {
		streamer.Stop(s.ID)
	})

	waitForStreamRunning(t, streamer, s.ID)

	if !mgr.StopIfGeneration(s.ID, 0) {
		t.Fatal("StopIfGeneration should return true when genID=0 skips generation check")
	}
	if got := mgr.Get(s.ID).State; got != StateStopped {
		t.Errorf("expected stopped state, got %s", got)
	}
	waitForStreamStopped(t, streamer, s.ID)
}

func TestDIDLDurationParsing(t *testing.T) {
	// Bare item with res duration attribute
	md := parseDIDL(`<item><title>Test</title><artist>Artist</artist><res duration="00:03:45.000">http://example.com/stream</res></item>`)
	if md.Duration != "00:03:45.000" {
		t.Errorf("expected duration '00:03:45.000', got '%s'", md.Duration)
	}

	// DIDL-Lite wrapper with res duration
	md = parseDIDL(`<DIDL-Lite><item><title>Wrapped</title><artist>Wrapper</artist><res duration="01:30:00.000">http://example.com/long</res></item></DIDL-Lite>`)
	if md.Duration != "01:30:00.000" {
		t.Errorf("expected duration '01:30:00.000', got '%s'", md.Duration)
	}

	// No duration attribute
	md = parseDIDL(`<DIDL-Lite><item><title>No Duration</title></item></DIDL-Lite>`)
	if md.Duration != "" {
		t.Errorf("expected empty duration, got '%s'", md.Duration)
	}
}

func TestDIDLProtocolInfoContentTypeParsing(t *testing.T) {
	md := parseDIDL(`<item><title>Test</title><res protocolInfo="http-get:*:audio/mpeg:*" duration="00:03:45">http://example.com/stream</res></item>`)
	if md.ContentType != "audio/mpeg" {
		t.Errorf("expected content type 'audio/mpeg', got %q", md.ContentType)
	}
}

func TestSeekPlayingTransitionsToStarting(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s := mgr.Create("http://source.local/test.flac", "")
	mgr.Play(s.ID)
	mgr.SetPlaying(s.ID)
	if mgr.Get(s.ID).State != StatePlaying {
		t.Fatalf("expected playing state, got %s", mgr.Get(s.ID).State)
	}

	mgr.Seek(s.ID, 30*time.Second)
	if mgr.Get(s.ID).State != StateStarting {
		t.Errorf("expected StateStarting after Seek on playing session, got %s", mgr.Get(s.ID).State)
	}
}

func TestSeekPausedStaysPaused(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s := mgr.Create("http://source.local/test.flac", "")
	mgr.Play(s.ID)
	mgr.SetPlaying(s.ID)
	mgr.Pause(s.ID)
	if mgr.Get(s.ID).State != StatePaused {
		t.Fatalf("expected paused state, got %s", mgr.Get(s.ID).State)
	}

	mgr.Seek(s.ID, 30*time.Second)
	if mgr.Get(s.ID).State != StatePaused {
		t.Errorf("expected StatePaused after Seek on paused session, got %s", mgr.Get(s.ID).State)
	}
}

func TestSeekStartingAllowsSetPlayingAccepted(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s := mgr.Create("http://source.local/test.flac", "")
	mgr.Play(s.ID)
	mgr.SetPlaying(s.ID)
	if mgr.Get(s.ID).State != StatePlaying {
		t.Fatalf("expected playing state, got %s", mgr.Get(s.ID).State)
	}

	mgr.Seek(s.ID, 30*time.Second)
	if mgr.Get(s.ID).State != StateStarting {
		t.Fatalf("expected StateStarting after Seek, got %s", mgr.Get(s.ID).State)
	}

	if !mgr.SetPlayingAccepted(s.ID) {
		t.Fatal("SetPlayingAccepted should return true when state is StateStarting")
	}
	if mgr.Get(s.ID).State != StatePlaying {
		t.Errorf("expected StatePlaying after SetPlayingAccepted, got %s", mgr.Get(s.ID).State)
	}
}

func TestSeekStartingKeepsStarting(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s := mgr.Create("http://source.local/test.flac", "")
	mgr.Play(s.ID)
	if mgr.Get(s.ID).State != StateStarting {
		t.Fatalf("expected StateStarting after Play, got %s", mgr.Get(s.ID).State)
	}

	mgr.Seek(s.ID, 30*time.Second)
	if mgr.Get(s.ID).State != StateStarting {
		t.Errorf("expected StateStarting after Seek on starting session, got %s", mgr.Get(s.ID).State)
	}

	if !mgr.SetPlayingAccepted(s.ID) {
		t.Fatal("SetPlayingAccepted should return true when state is StateStarting")
	}
	if mgr.Get(s.ID).State != StatePlaying {
		t.Errorf("expected StatePlaying after SetPlayingAccepted, got %s", mgr.Get(s.ID).State)
	}
}

func TestSetPlayingAcceptedIfGenerationMatchingGen(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s := mgr.Create("http://source.local/test.flac", "")
	mgr.Play(s.ID)
	if mgr.Get(s.ID).State != StateStarting {
		t.Fatalf("expected StateStarting after Play, got %s", mgr.Get(s.ID).State)
	}

	mgr.SetSessionGenID(s.ID, 42)
	if !mgr.SetPlayingAcceptedIfGeneration(s.ID, 42) {
		t.Fatal("SetPlayingAcceptedIfGeneration should return true for matching gen")
	}
	if mgr.Get(s.ID).State != StatePlaying {
		t.Errorf("expected StatePlaying, got %s", mgr.Get(s.ID).State)
	}
}

func TestSetPlayingAcceptedIfGenerationRejectsStaleGen(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s := mgr.Create("http://source.local/test.flac", "")
	mgr.Play(s.ID)
	if mgr.Get(s.ID).State != StateStarting {
		t.Fatalf("expected StateStarting after Play, got %s", mgr.Get(s.ID).State)
	}

	mgr.SetSessionGenID(s.ID, 42)
	if mgr.SetPlayingAcceptedIfGeneration(s.ID, 99) {
		t.Fatal("SetPlayingAcceptedIfGeneration should return false for stale gen")
	}
	if mgr.Get(s.ID).State != StateStarting {
		t.Errorf("state should remain StateStarting after stale gen rejection, got %s", mgr.Get(s.ID).State)
	}
}

func TestSetPlayingAcceptedIfGenerationZeroGenSkipsCheck(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s := mgr.Create("http://source.local/test.flac", "")
	mgr.Play(s.ID)
	if mgr.Get(s.ID).State != StateStarting {
		t.Fatalf("expected StateStarting after Play, got %s", mgr.Get(s.ID).State)
	}

	// genID=0 should skip generation check (backward compatibility)
	if !mgr.SetPlayingAcceptedIfGeneration(s.ID, 0) {
		t.Fatal("SetPlayingAcceptedIfGeneration should return true when genID=0")
	}
	if mgr.Get(s.ID).State != StatePlaying {
		t.Errorf("expected StatePlaying, got %s", mgr.Get(s.ID).State)
	}
}

func TestSetPlayingAcceptedIfGenerationWrongState(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s := mgr.Create("http://source.local/test.flac", "")
	// State is Loaded, not Starting
	mgr.SetSessionGenID(s.ID, 42)
	if mgr.SetPlayingAcceptedIfGeneration(s.ID, 42) {
		t.Fatal("SetPlayingAcceptedIfGeneration should return false when not StateStarting")
	}
	if mgr.Get(s.ID).State != StateLoaded {
		t.Errorf("state should remain StateLoaded, got %s", mgr.Get(s.ID).State)
	}
}

func TestSeekFromStartingInvalidatesOldGen(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s := mgr.Create("http://source.local/test.flac", "")
	mgr.Play(s.ID)
	mgr.SetSessionGenID(s.ID, 42)
	if mgr.Get(s.ID).State != StateStarting {
		t.Fatalf("expected StateStarting after Play, got %s", mgr.Get(s.ID).State)
	}

	// Seek should invalidate the old generation
	mgr.Seek(s.ID, 30*time.Second)
	if mgr.Get(s.ID).State != StateStarting {
		t.Fatalf("expected StateStarting after Seek, got %s", mgr.Get(s.ID).State)
	}

	// Old gen should now be rejected
	if mgr.SetPlayingAcceptedIfGeneration(s.ID, 42) {
		t.Fatal("SetPlayingAcceptedIfGeneration should return false for invalidated old gen after Seek")
	}

	// Set a new gen to simulate a new stream generation
	mgr.SetSessionGenID(s.ID, 99)
	if !mgr.SetPlayingAcceptedIfGeneration(s.ID, 99) {
		t.Fatal("SetPlayingAcceptedIfGeneration should return true for new gen after Seek")
	}
	if mgr.Get(s.ID).State != StatePlaying {
		t.Errorf("expected StatePlaying after new gen acceptance, got %s", mgr.Get(s.ID).State)
	}
}

func TestSetErrorIfGenerationMatchingGen(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s := mgr.Create("http://source.local/test.flac", "")
	mgr.Play(s.ID)
	mgr.SetPlaying(s.ID)
	if mgr.Get(s.ID).State != StatePlaying {
		t.Fatalf("expected playing state, got %s", mgr.Get(s.ID).State)
	}

	mgr.SetSessionGenID(s.ID, 42)
	if !mgr.SetErrorIfGeneration(s.ID, 42, "playback error") {
		t.Fatal("SetErrorIfGeneration should return true for matching gen")
	}
	got := mgr.Get(s.ID)
	if got.State != StateError {
		t.Errorf("expected StateError, got %s", got.State)
	}
	if got.Error != "playback error" {
		t.Errorf("expected error 'playback error', got '%s'", got.Error)
	}
}

func TestSetErrorIfGenerationRejectsStaleGen(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s := mgr.Create("http://source.local/test.flac", "")
	mgr.Play(s.ID)
	mgr.SetPlaying(s.ID)
	if mgr.Get(s.ID).State != StatePlaying {
		t.Fatalf("expected playing state, got %s", mgr.Get(s.ID).State)
	}

	mgr.SetSessionGenID(s.ID, 42)
	if mgr.SetErrorIfGeneration(s.ID, 99, "stale error") {
		t.Fatal("SetErrorIfGeneration should return false for stale gen")
	}
	got := mgr.Get(s.ID)
	if got.State != StatePlaying {
		t.Errorf("state should remain StatePlaying after stale gen rejection, got %s", got.State)
	}
	if got.Error != "" {
		t.Errorf("error should remain empty after stale gen rejection, got '%s'", got.Error)
	}
}

func TestMarkPausedIfGenerationMatchingGenFromPlaying(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s := mgr.Create("http://source.local/test.flac", "")
	mgr.Play(s.ID)
	mgr.SetPlaying(s.ID)
	if mgr.Get(s.ID).State != StatePlaying {
		t.Fatalf("expected playing state, got %s", mgr.Get(s.ID).State)
	}

	mgr.SetSessionGenID(s.ID, 42)
	if !mgr.MarkPausedIfGeneration(s.ID, 42) {
		t.Fatal("MarkPausedIfGeneration should return true for matching gen")
	}
	if mgr.Get(s.ID).State != StatePaused {
		t.Errorf("expected StatePaused, got %s", mgr.Get(s.ID).State)
	}
}

func TestMarkPausedIfGenerationMatchingGenFromStarting(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s := mgr.Create("http://source.local/test.flac", "")
	mgr.Play(s.ID)
	if mgr.Get(s.ID).State != StateStarting {
		t.Fatalf("expected starting state, got %s", mgr.Get(s.ID).State)
	}

	mgr.SetSessionGenID(s.ID, 42)
	if !mgr.MarkPausedIfGeneration(s.ID, 42) {
		t.Fatal("MarkPausedIfGeneration should return true for matching gen from starting")
	}
	if mgr.Get(s.ID).State != StatePaused {
		t.Errorf("expected StatePaused, got %s", mgr.Get(s.ID).State)
	}
}

func TestMarkPausedIfGenerationRejectsStaleGen(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s := mgr.Create("http://source.local/test.flac", "")
	mgr.Play(s.ID)
	mgr.SetPlaying(s.ID)
	if mgr.Get(s.ID).State != StatePlaying {
		t.Fatalf("expected playing state, got %s", mgr.Get(s.ID).State)
	}

	mgr.SetSessionGenID(s.ID, 42)
	if mgr.MarkPausedIfGeneration(s.ID, 99) {
		t.Fatal("MarkPausedIfGeneration should return false for stale gen")
	}
	if mgr.Get(s.ID).State != StatePlaying {
		t.Errorf("state should remain StatePlaying after stale gen rejection, got %s", mgr.Get(s.ID).State)
	}
}

func TestSafeURLRedactsToken(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"http://example.com/song.mp3", "http://example.com/song.mp3"},
		{"http://user:pass@example.com/song.mp3", "http://***@example.com/song.mp3"},
		{"http://bridge:8787/live/abc.opus?token=secret123", "http://bridge:8787/live/abc.opus?..."},
		{"http://example.com/path?a=1&token=secret&b=2", "http://example.com/path?..."},
		{"http://example.com/path?token=secret", "http://example.com/path?..."},
	}
	for _, tc := range tests {
		got := safeURL(tc.input)
		if got != tc.expected {
			t.Errorf("safeURL(%q) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}

func TestSetErrorIfNoGenerationSetsErrorWhenGenZero(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s := mgr.Create("http://source.local/test.flac", "")
	mgr.Play(s.ID)
	mgr.SetPlaying(s.ID)
	if mgr.Get(s.ID).State != StatePlaying {
		t.Fatalf("expected playing state, got %s", mgr.Get(s.ID).State)
	}

	if gen := mgr.CurrentGenID(s.ID); gen != 0 {
		t.Fatalf("expected gen 0, got %d", gen)
	}

	if !mgr.SetErrorIfNoGeneration(s.ID, "seek without generation") {
		t.Fatal("SetErrorIfNoGeneration should return true when gen is zero")
	}
	got := mgr.Get(s.ID)
	if got.State != StateError {
		t.Errorf("expected StateError, got %s", got.State)
	}
	if got.Error != "seek without generation" {
		t.Errorf("expected error message, got '%s'", got.Error)
	}
}

func TestSetErrorIfNoGenerationRejectsWhenGenNonzero(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s := mgr.Create("http://source.local/test.flac", "")
	mgr.Play(s.ID)
	mgr.SetPlaying(s.ID)
	if mgr.Get(s.ID).State != StatePlaying {
		t.Fatalf("expected playing state, got %s", mgr.Get(s.ID).State)
	}

	mgr.SetSessionGenID(s.ID, 42)

	if mgr.SetErrorIfNoGeneration(s.ID, "should not apply") {
		t.Fatal("SetErrorIfNoGeneration should return false when gen is nonzero")
	}
	got := mgr.Get(s.ID)
	if got.State != StatePlaying {
		t.Errorf("state should remain StatePlaying, got %s", got.State)
	}
	if got.Error != "" {
		t.Errorf("error should remain empty, got '%s'", got.Error)
	}
}

func TestStopWithErrorIfGenerationActiveSetsError(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s := mgr.Create("http://source.local/test.flac", "")
	if err := mgr.Play(s.ID); err != nil {
		t.Fatalf("Play failed: %v", err)
	}
	mgr.SetSessionGenID(s.ID, 42)

	if !mgr.StopWithErrorIfGenerationActive(s.ID, 42, "play failed") {
		t.Fatal("StopWithErrorIfGenerationActive should accept current active generation")
	}
	got := mgr.Get(s.ID)
	if got.State != StateError {
		t.Errorf("expected StateError, got %s", got.State)
	}
	if got.Error != "play failed" {
		t.Errorf("expected error message, got %q", got.Error)
	}
}

func TestStopWithErrorIfGenerationActiveIgnoresStoppedSession(t *testing.T) {
	cfg := config.DefaultConfig()
	mgr := NewManager(&cfg, stream.NewStreamer(&cfg))

	s := mgr.Create("http://source.local/test.flac", "")
	if err := mgr.Play(s.ID); err != nil {
		t.Fatalf("Play failed: %v", err)
	}
	mgr.SetSessionGenID(s.ID, 42)
	if err := mgr.Stop(s.ID); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if mgr.StopWithErrorIfGenerationActive(s.ID, 42, "late play failed") {
		t.Fatal("StopWithErrorIfGenerationActive should reject stopped session")
	}
	got := mgr.Get(s.ID)
	if got.State != StateStopped {
		t.Errorf("state should remain StateStopped, got %s", got.State)
	}
	if got.Error != "" {
		t.Errorf("error should remain empty, got %q", got.Error)
	}
}
