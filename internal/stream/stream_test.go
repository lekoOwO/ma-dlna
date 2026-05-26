package stream

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/leko/ma-dlna/internal/config"
)

func TestContentTypeMapping(t *testing.T) {
	tests := []struct {
		format   string
		expected string
	}{
		{"mp3", "audio/mpeg"},
		{"opus", "audio/ogg"},
		{"ogg", "audio/ogg"},
		{"flac", "audio/flac"},
		{"aac", "audio/aac"},
		{"wav", "audio/wav"},
		{"unknown", "audio/unknown"},
	}
	for _, tc := range tests {
		got := contentTypeForFormat(tc.format)
		if got != tc.expected {
			t.Errorf("format %s: expected %s, got %s", tc.format, tc.expected, got)
		}
	}
}

func TestExtractSessionID(t *testing.T) {
	tests := []struct {
		path     string
		expected string
	}{
		{"abc123.mp3", "abc123"},
		{"abc123.opus", "abc123"},
		{"abc123.ogg", "abc123"},
		{"abc123", "abc123"},
		{"abc.def.ghi.mp3", "abc.def.ghi"},
	}
	for _, tc := range tests {
		got := extractSessionID(tc.path)
		if got != tc.expected {
			t.Errorf("path %s: expected %s, got %s", tc.path, tc.expected, got)
		}
	}
}

func TestBuildFFmpegArgsDefault(t *testing.T) {
	cfg := config.DefaultConfig()
	st := &stream{
		sessionID: "test123",
		sourceURI: "http://source.local/audio.flac",
		ffmpegCfg: cfg.FFmpeg,
	}

	args := st.buildFFmpegArgs(0)

	findArg := func(name string) int {
		for i, a := range args {
			if a == name {
				return i
			}
		}
		return -1
	}

	if i := findArg("-codec:a"); i < 0 || args[i+1] != "libopus" {
		t.Error("expected libopus codec")
	}
	if i := findArg("-b:a"); i < 0 || args[i+1] != "192k" {
		t.Error("expected 192k bitrate")
	}
	if i := findArg("-f"); i < 0 || args[i+1] != "opus" {
		t.Error("expected opus format")
	}
	if findArg("pipe:1") < 0 {
		t.Error("expected pipe:1 output")
	}
	if findArg("-i") < 0 {
		t.Error("expected -i flag")
	}
}

func TestBuildFFmpegArgsCustom(t *testing.T) {
	st := &stream{
		sessionID: "test456",
		sourceURI: "http://source.local/audio.mp3",
		ffmpegCfg: config.FFmpegConfig{
			OutputFormat:    "mp3",
			Codec:           "libmp3lame",
			Bitrate:         "320k",
			SampleRate:      44100,
			Channels:        2,
			Reconnect:       false,
			ExtraInputArgs:  []string{"-analyzeduration", "10000000"},
			ExtraOutputArgs: []string{"-compression_level", "2"},
		},
	}

	args := st.buildFFmpegArgs(0)

	findArg := func(name string) int {
		for i, a := range args {
			if a == name {
				return i
			}
		}
		return -1
	}

	if i := findArg("-codec:a"); i < 0 || args[i+1] != "libmp3lame" {
		t.Error("expected libmp3lame codec")
	}
	if i := findArg("-b:a"); i < 0 || args[i+1] != "320k" {
		t.Error("expected 320k bitrate")
	}
	if i := findArg("-ac"); i < 0 || args[i+1] != "2" {
		t.Error("expected stereo channels")
	}
	if i := findArg("-ar"); i < 0 || args[i+1] != "44100" {
		t.Error("expected 44100 sample rate")
	}
	if findArg("-reconnect") >= 0 {
		t.Error("reconnect should not be present when disabled")
	}
	if i := findArg("-analyzeduration"); i < 0 {
		t.Error("expected extra input args")
	}
	if i := findArg("-compression_level"); i < 0 {
		t.Error("expected extra output args")
	}
}

func TestRingBufferWriteRead(t *testing.T) {
	rb := NewRingBuffer(1024)

	data := []byte("hello world")
	rb.Write(data)

	pos := rb.WritePosition()
	if pos != 11 {
		t.Errorf("expected write pos 11, got %d", pos)
	}

	// Read from beginning
	buf := make([]byte, 20)
	n, _ := rb.Read(0, buf)
	if n != 11 {
		t.Errorf("expected 11 bytes, got %d", n)
	}
	if string(buf[:n]) != "hello world" {
		t.Errorf("expected 'hello world', got '%s'", string(buf[:n]))
	}
}

func TestRingBufferOverflow(t *testing.T) {
	rb := NewRingBuffer(8)

	// Write more than buffer size
	rb.Write([]byte("abcdefghij"))

	pos := rb.WritePosition()
	if pos != 10 {
		t.Errorf("expected write pos 10, got %d", pos)
	}

	// Read should return only last 8 bytes since overflow wraps
	buf := make([]byte, 8)
	n, _ := rb.Read(2, buf) // Skip first 2 bytes
	if n != 8 {
		t.Errorf("expected 8 bytes, got %d", n)
	}
}

func TestStreamerStartStop(t *testing.T) {
	cfg := config.DefaultConfig()
	streamer := NewStreamer(&cfg)

	err := streamer.Start("test-session", "http://source.local/test.mp3")
	if err != nil {
		t.Fatalf("start failed: %v", err)
	}
	if !streamer.IsRunning("test-session") {
		t.Error("stream should be running")
	}

	streamer.Stop("test-session")
	if streamer.IsRunning("test-session") {
		t.Error("stream should not be running after stop")
	}
}

func TestStreamerStopIfGenerationStopsMatchingGeneration(t *testing.T) {
	cfg := streamTestConfig(t)
	streamer := NewStreamer(&cfg)
	var genID uint64
	streamer.SetGenStartCallback(func(_ string, gid uint64) {
		genID = gid
	})

	if err := streamer.Start("matching-gen", "http://source.local/test.mp3"); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	t.Cleanup(func() {
		streamer.Stop("matching-gen")
	})
	waitForStreamRunning(t, streamer, "matching-gen")
	if genID == 0 {
		t.Fatal("expected start callback to record generation")
	}

	if !streamer.StopIfGeneration("matching-gen", genID) {
		t.Fatal("StopIfGeneration should accept the current generation")
	}
	waitForStreamStopped(t, streamer, "matching-gen")
	if streamExists(streamer, "matching-gen") {
		t.Fatal("stream should be removed after matching generation stop")
	}
}

func TestStreamerStopIfGenerationRejectsOldGenerationAfterResume(t *testing.T) {
	cfg := streamTestConfig(t)
	streamer := NewStreamer(&cfg)
	var gens []uint64
	streamer.SetGenStartCallback(func(_ string, gid uint64) {
		gens = append(gens, gid)
	})

	const sessionID = "stale-gen"
	if err := streamer.Start(sessionID, "http://source.local/test.mp3"); err != nil {
		t.Fatalf("start failed: %v", err)
	}
	t.Cleanup(func() {
		streamer.Stop(sessionID)
	})
	waitForStreamRunning(t, streamer, sessionID)
	if len(gens) != 1 {
		t.Fatalf("expected one generation after start, got %d", len(gens))
	}
	oldGen := gens[0]

	streamer.Seek(sessionID, 5*time.Second)
	streamer.Resume(sessionID)
	waitForStreamRunning(t, streamer, sessionID)
	if len(gens) < 3 {
		t.Fatalf("expected seek and resume to create new generations, got %d", len(gens))
	}

	if streamer.StopIfGeneration(sessionID, oldGen) {
		t.Fatal("StopIfGeneration should reject an old generation")
	}
	if !streamer.IsRunning(sessionID) {
		t.Fatal("new generation should still be running")
	}
	if !streamExists(streamer, sessionID) {
		t.Fatal("stream should remain registered after stale generation rejection")
	}
}

func TestStreamerPauseUsesPresentationOffsetBeforePlaybackAccepted(t *testing.T) {
	cfg := config.DefaultConfig()
	streamer := NewStreamer(&cfg)
	const sessionID = "presentation-pause"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	close(done)
	gen := &streamGeneration{
		ctx:          ctx,
		cancel:       cancel,
		started:      make(chan struct{}),
		done:         done,
		firstClient:  make(chan struct{}),
		ringBuf:      NewRingBuffer(cfg.Stream.RingBufferBytes),
		clients:      make(map[string]*clientWriter),
		offset:       5 * time.Second,
		startTime:    time.Now().Add(-15 * time.Second),
		genID:        1,
		initBufLimit: int64(cfg.Stream.InitSegmentBytes),
	}
	gen.ffmpegTime.Store(int64(15 * time.Second))
	gen.active.Store(true)

	st := &stream{
		sessionID:      sessionID,
		sourceURI:      "http://source.local/song.mp3",
		ffmpegCfg:      cfg.FFmpeg,
		startupTimeout: time.Second,
		gen:            gen,
	}
	st.active.Store(true)
	st.runsInFlight.Store(1)
	st.currentGenID.Store(1)
	streamer.streams[sessionID] = st

	pos := streamer.Pause(sessionID)
	if pos != 5*time.Second {
		t.Fatalf("pause before playback acceptance should keep presentation offset 5s, got %s", pos)
	}
}

func TestStreamerElapsedKeepsAcceptedPresentationPositionWhenRunStops(t *testing.T) {
	cfg := config.DefaultConfig()
	streamer := NewStreamer(&cfg)
	const sessionID = "presentation-stopped-run"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	close(done)
	gen := &streamGeneration{
		ctx:          ctx,
		cancel:       cancel,
		started:      make(chan struct{}),
		done:         done,
		firstClient:  make(chan struct{}),
		ringBuf:      NewRingBuffer(cfg.Stream.RingBufferBytes),
		clients:      make(map[string]*clientWriter),
		offset:       5 * time.Second,
		startTime:    time.Now().Add(-20 * time.Second),
		genID:        1,
		initBufLimit: int64(cfg.Stream.InitSegmentBytes),
	}
	gen.active.Store(true)

	st := &stream{
		sessionID:      sessionID,
		sourceURI:      "http://source.local/song.mp3",
		ffmpegCfg:      cfg.FFmpeg,
		startupTimeout: time.Second,
		gen:            gen,
	}
	st.active.Store(true)
	st.runsInFlight.Store(1)
	st.currentGenID.Store(1)
	streamer.streams[sessionID] = st

	if !streamer.MarkPlaybackAcceptedIfGeneration(sessionID, 1) {
		t.Fatal("expected presentation acceptance")
	}
	time.Sleep(20 * time.Millisecond)
	acceptedPos := streamer.Elapsed(sessionID)
	if acceptedPos <= 5*time.Second {
		t.Fatalf("expected presentation position to advance after acceptance, got %s", acceptedPos)
	}

	st.runsInFlight.Store(0)
	if pos := streamer.Elapsed(sessionID); pos != acceptedPos {
		t.Fatalf("stopped run should keep last accepted presentation position %s, got %s", acceptedPos, pos)
	}
}

func TestStreamerInitSegmentLimitHonorsReplayCap(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Stream.InitSegmentBytes = 262144
	cfg.Stream.MaxReplayBytes = 65536

	streamer := NewStreamer(&cfg)
	if got := streamer.initSegmentLimit(); got != 65536 {
		t.Fatalf("init segment limit should be capped to 64KB, got %d", got)
	}

	cfg.Stream.MaxReplayBytes = 0
	streamer = NewStreamer(&cfg)
	if got := streamer.initSegmentLimit(); got != 262144 {
		t.Fatalf("init segment limit should be uncapped when max_replay_bytes=0, got %d", got)
	}
}

func TestAllowReplayBytesExhaustsPositiveReplayCap(t *testing.T) {
	remaining := 65536
	if got := allowReplayBytes(32768, 65536, &remaining); got != 32768 {
		t.Fatalf("first replay allowance = %d, want 32768", got)
	}
	if got := allowReplayBytes(65536, 65536, &remaining); got != 32768 {
		t.Fatalf("second replay allowance = %d, want remaining 32768", got)
	}
	if got := allowReplayBytes(1, 65536, &remaining); got != 0 {
		t.Fatalf("exhausted replay allowance = %d, want 0", got)
	}

	remaining = 0
	if got := allowReplayBytes(1024, 0, &remaining); got != 1024 {
		t.Fatalf("uncapped replay allowance = %d, want 1024", got)
	}
}

func TestOldGenerationExitDoesNotClearCurrentRunTracking(t *testing.T) {
	dir := t.TempDir()
	releasePath := filepath.Join(dir, "release")
	fakeFFmpeg := filepath.Join(dir, "ffmpeg")
	script := `#!/bin/sh
while [ ! -f "$FAKE_FFMPEG_RELEASE" ]; do
  sleep 0.01
done
exit 0
`
	if err := os.WriteFile(fakeFFmpeg, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake ffmpeg: %v", err)
	}
	t.Setenv("FAKE_FFMPEG_RELEASE", releasePath)

	cfg := config.DefaultConfig()
	cfg.FFmpeg.Binary = fakeFFmpeg

	oldCtx, oldCancel := context.WithCancel(context.Background())
	defer oldCancel()
	oldGen := &streamGeneration{
		ctx:          oldCtx,
		cancel:       oldCancel,
		started:      make(chan struct{}),
		done:         make(chan struct{}),
		firstClient:  make(chan struct{}),
		ringBuf:      NewRingBuffer(cfg.Stream.RingBufferBytes),
		clients:      make(map[string]*clientWriter),
		genID:        1,
		initBufLimit: int64(cfg.Stream.InitSegmentBytes),
	}
	st := &stream{
		sessionID:      "late-old-generation",
		sourceURI:      "http://source.local/song.mp3",
		ffmpegCfg:      cfg.FFmpeg,
		startupTimeout: time.Second,
		gen:            oldGen,
	}
	st.active.Store(true)
	st.currentGenID.Store(oldGen.genID)
	st.runsInFlight.Store(1)

	go st.run(oldGen)
	select {
	case <-oldGen.started:
	case <-time.After(time.Second):
		t.Fatal("old generation did not start")
	}

	newCtx, newCancel := context.WithCancel(context.Background())
	defer newCancel()
	newGen := &streamGeneration{
		ctx:          newCtx,
		cancel:       newCancel,
		started:      make(chan struct{}),
		done:         make(chan struct{}),
		firstClient:  make(chan struct{}),
		ringBuf:      NewRingBuffer(cfg.Stream.RingBufferBytes),
		clients:      make(map[string]*clientWriter),
		startTime:    time.Now(),
		genID:        2,
		initBufLimit: int64(cfg.Stream.InitSegmentBytes),
	}
	newGen.active.Store(true)
	st.genMu.Lock()
	st.gen = newGen
	st.currentGenID.Store(newGen.genID)
	st.genMu.Unlock()
	st.runsInFlight.Store(1)

	if err := os.WriteFile(releasePath, []byte("release"), 0o644); err != nil {
		t.Fatalf("release fake ffmpeg: %v", err)
	}
	select {
	case <-oldGen.done:
	case <-time.After(time.Second):
		t.Fatal("old generation did not exit")
	}

	if got := st.runsInFlight.Load(); got != 1 {
		t.Fatalf("old generation cleanup cleared current run tracking: got runsInFlight=%d, want 1", got)
	}
}

func TestStreamerServeInvalidPath(t *testing.T) {
	cfg := config.DefaultConfig()
	streamer := NewStreamer(&cfg)

	w := &testResponseWriter{header: make(http.Header)}
	r, _ := http.NewRequest("GET", "/live/", nil)
	streamer.ServeHTTP(w, r)

	if w.status != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.status)
	}
}

func TestStreamerServeMethodNotAllowed(t *testing.T) {
	cfg := config.DefaultConfig()
	streamer := NewStreamer(&cfg)

	w := &testResponseWriter{header: make(http.Header)}
	r, _ := http.NewRequest("POST", "/live/test.mp3", nil)
	streamer.ServeHTTP(w, r)

	if w.status != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.status)
	}
}

func TestStreamerServeNotFound(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Security.RequireStreamToken = false
	streamer := NewStreamer(&cfg)

	w := &testResponseWriter{header: make(http.Header)}
	r, _ := http.NewRequest("GET", "/live/nonexistent.mp3", nil)
	streamer.ServeHTTP(w, r)

	if w.status != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.status)
	}
}

func TestStreamerHeadRequest(t *testing.T) {
	cfg := config.DefaultConfig()
	streamer := NewStreamer(&cfg)

	if err := streamer.Start("head-test", "http://source.local/test.mp3"); err != nil {
		t.Fatal(err)
	}
	defer streamer.Stop("head-test")

	// HEAD should return 200 with Content-Type
	w := &testResponseWriter{header: make(http.Header)}
	r, _ := http.NewRequest("HEAD", "/live/head-test.mp3", nil)
	streamer.ServeHTTP(w, r)

	if w.status != http.StatusOK {
		t.Errorf("expected 200, got %d", w.status)
	}
	if w.header.Get("Content-Type") != "audio/ogg" {
		t.Errorf("expected audio/ogg, got %s", w.header.Get("Content-Type"))
	}
}

func TestExtractSessionIDEdgeCases(t *testing.T) {
	if extractSessionID("") != "" {
		t.Error("empty path should return empty")
	}
	if extractSessionID(".mp3") != "" {
		t.Error("dot-only path should return empty")
	}
}

type testResponseWriter struct {
	header http.Header
	status int
	body   []byte
}

func (w *testResponseWriter) Header() http.Header {
	return w.header
}

func (w *testResponseWriter) Write(b []byte) (int, error) {
	w.body = append(w.body, b...)
	return len(b), nil
}

func (w *testResponseWriter) WriteHeader(status int) {
	w.status = status
}

func streamTestConfig(t *testing.T) config.Config {
	t.Helper()

	cfg := config.DefaultConfig()
	cfg.FFmpeg.Binary = writeFakeFFmpeg(t)
	return cfg
}

func writeFakeFFmpeg(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "ffmpeg")
	script := "#!/bin/sh\nexec sleep 3600\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake ffmpeg: %v", err)
	}
	return path
}

func waitForStreamRunning(t *testing.T, streamer *Streamer, sessionID string) {
	t.Helper()
	waitForCondition(t, 500*time.Millisecond, "stream to be running", func() bool {
		return streamer.IsRunning(sessionID)
	})
}

func waitForStreamStopped(t *testing.T, streamer *Streamer, sessionID string) {
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

func streamExists(streamer *Streamer, sessionID string) bool {
	streamer.mu.Lock()
	defer streamer.mu.Unlock()
	_, ok := streamer.streams[sessionID]
	return ok
}
