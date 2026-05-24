package stream

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/leko/ma-dlna/internal/config"
)

type TokenValidator func(sessionID, token string) bool

type FirstClientCallback func(sessionID string)

type Streamer struct {
	cfg              *config.Config
	mu               sync.Mutex
	streams          map[string]*stream
	tokenValidator   TokenValidator
	firstClientCB    FirstClientCallback
}

type stream struct {
	sessionID     string
	sourceURI     string
	ringBuf       *RingBuffer
	cmd           *exec.Cmd
	cancel        context.CancelFunc
	active        atomic.Bool
	clients       map[string]*clientWriter
	clientsMu     sync.Mutex
	started       chan struct{}
	err           error
	ffmpegCfg     config.FFmpegConfig
	startTime     time.Time
	resumeOffset  time.Duration
	ffmpegTime    atomic.Int64 // output media time in nanoseconds (from ffmpeg progress)
}

type clientWriter struct {
	id     string
	w      http.ResponseWriter
	flusher http.Flusher
	ctx    context.Context
	cancel context.CancelFunc
}

func NewStreamer(cfg *config.Config) *Streamer {
	return &Streamer{
		cfg:     cfg,
		streams: make(map[string]*stream),
	}
}

func (s *Streamer) SetTokenValidator(v TokenValidator) {
	s.tokenValidator = v
}

func (s *Streamer) Start(sessionID, sourceURI string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.streams[sessionID]; ok && existing.active.Load() {
		slog.Debug("Stream already running", "session_id", sessionID)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	st := &stream{
		sessionID: sessionID,
		sourceURI: sourceURI,
		ringBuf:   NewRingBuffer(s.cfg.Stream.RingBufferBytes),
		clients:   make(map[string]*clientWriter),
		started:   make(chan struct{}),
		cancel:    cancel,
		ffmpegCfg: s.cfg.FFmpeg,
	}
	st.active.Store(true)
	st.startTime = time.Now()
	st.resumeOffset = 0
	s.streams[sessionID] = st

	go st.run(ctx)
	return nil
}

func (s *Streamer) Stop(sessionID string) {
	s.mu.Lock()
	st, ok := s.streams[sessionID]
	if !ok {
		s.mu.Unlock()
		return
	}
	delete(s.streams, sessionID)
	s.mu.Unlock()

	if st.active.Swap(false) {
		st.cancel()
		if st.cmd != nil && st.cmd.Process != nil {
			st.cmd.Process.Kill()
		}
		st.clientsMu.Lock()
		for _, c := range st.clients {
			c.cancel()
		}
		st.clientsMu.Unlock()
		slog.Info("Stream stopped", "session_id", sessionID)
	}
}

func (s *Streamer) Pause(sessionID string) time.Duration {
	s.mu.Lock()
	st, ok := s.streams[sessionID]
	s.mu.Unlock()
	if !ok {
		return 0
	}
	elapsed := s.Elapsed(sessionID)
	s.restartWithOffset(st, sessionID, elapsed)
	slog.Info("Stream paused", "session_id", sessionID, "position", elapsed.Round(time.Second))
	return elapsed
}

func (s *Streamer) Seek(sessionID string, offset time.Duration) {
	s.mu.Lock()
	st, ok := s.streams[sessionID]
	s.mu.Unlock()
	if !ok {
		return
	}
	s.restartWithOffset(st, sessionID, offset)
	slog.Info("Stream seek", "session_id", sessionID, "to", offset.Round(time.Second))
}

// restartWithOffset kills ffmpeg, resets the stream, and sets resume offset for restart.
func (s *Streamer) restartWithOffset(st *stream, sessionID string, offset time.Duration) {
	if st.active.Swap(false) {
		st.cancel()
		if st.cmd != nil && st.cmd.Process != nil {
			st.cmd.Process.Kill()
		}
		st.clientsMu.Lock()
		for _, c := range st.clients {
			c.cancel()
		}
		st.clientsMu.Unlock()
	}
	st.active.Store(true)
	_, cancel := context.WithCancel(context.Background())
	st.cancel = cancel
	st.resumeOffset = offset
	st.started = make(chan struct{})
	st.clients = make(map[string]*clientWriter)
}

func (s *Streamer) Resume(sessionID string) {
	s.mu.Lock()
	st, ok := s.streams[sessionID]
	s.mu.Unlock()
	if !ok || !st.active.Load() {
		return
	}
	resumeCtx, resumeCancel := context.WithCancel(context.Background())
	st.cancel = resumeCancel
	st.startTime = time.Now()
	go st.run(resumeCtx)
	slog.Info("Stream resuming", "session_id", sessionID, "offset", st.resumeOffset.Round(time.Second))
}

func (s *Streamer) Elapsed(sessionID string) time.Duration {
	s.mu.Lock()
	st, ok := s.streams[sessionID]
	s.mu.Unlock()
	if !ok {
		return 0
	}
	ft := st.ffmpegTime.Load()
	if ft > 0 {
		return st.resumeOffset + time.Duration(ft)
	}
	if st.startTime.IsZero() {
		return 0
	}
	return st.resumeOffset + time.Since(st.startTime)
}

func (s *Streamer) IsRunning(sessionID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.streams[sessionID]; ok {
		return st.active.Load()
	}
	return false
}

func (s *Streamer) SetFirstClientCallback(cb FirstClientCallback) {
	s.firstClientCB = cb
}

func (s *Streamer) TotalClients() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, st := range s.streams {
		st.clientsMu.Lock()
		total += len(st.clients)
		st.clientsMu.Unlock()
	}
	return total
}

func (s *Streamer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/live/")
	sessionID := extractSessionID(path)
	if sessionID == "" {
		http.Error(w, "Invalid stream path", http.StatusBadRequest)
		return
	}
	token := r.URL.Query().Get("token")

	if s.tokenValidator != nil && !s.tokenValidator(sessionID, token) {
		http.Error(w, "Invalid or missing token", http.StatusForbidden)
		return
	}

	s.mu.Lock()
	st, ok := s.streams[sessionID]
	s.mu.Unlock()

	if !ok || !st.active.Load() {
		http.Error(w, "Stream not available", http.StatusNotFound)
		return
	}

	contentType := contentTypeForFormat(st.ffmpegCfg.OutputFormat)

	if r.Method == http.MethodHead {
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Accept-Ranges", "none")
		w.WriteHeader(http.StatusOK)
		return
	}

	select {
	case <-st.started:
	case <-time.After(time.Duration(s.cfg.Stream.StartupTimeoutSeconds) * time.Second):
		http.Error(w, "Stream startup timeout", http.StatusServiceUnavailable)
		return
	}

	if st.err != nil {
		http.Error(w, "Stream error", http.StatusInternalServerError)
		return
	}

	clientID := generateClientID()
	ctx, cancel := context.WithCancel(r.Context())

	cw := &clientWriter{
		id:     clientID,
		w:      w,
		ctx:    ctx,
		cancel: cancel,
	}

	if f, ok := w.(http.Flusher); ok {
		cw.flusher = f
	}

	st.clientsMu.Lock()
	if len(st.clients) >= s.cfg.Stream.MaxClientsPerSession {
		st.clientsMu.Unlock()
		http.Error(w, "Too many clients", http.StatusTooManyRequests)
		return
	}
	st.clients[clientID] = cw
	isFirst := len(st.clients) == 1
	st.clientsMu.Unlock()

	if isFirst && s.firstClientCB != nil {
		s.firstClientCB(sessionID)
	}

	slog.Info("Client attached to stream", "session_id", sessionID, "client_id", clientID)

	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Accept-Ranges", "none")
	w.WriteHeader(http.StatusOK)

	if cw.flusher != nil {
		cw.flusher.Flush()
	}

	<-ctx.Done()

	st.clientsMu.Lock()
	delete(st.clients, clientID)
	remaining := len(st.clients)
	st.clientsMu.Unlock()

	slog.Info("Client disconnected from stream", "session_id", sessionID, "client_id", clientID, "remaining", remaining)

	if remaining == 0 {
		go func() {
			time.Sleep(time.Duration(s.cfg.Stream.NoClientGraceSeconds) * time.Second)
			st.clientsMu.Lock()
			if len(st.clients) == 0 {
				st.clientsMu.Unlock()
				slog.Info("No clients remaining, stopping stream", "session_id", sessionID)
				s.Stop(sessionID)
				return
			}
			st.clientsMu.Unlock()
		}()
	}
}

func (st *stream) run(ctx context.Context) {
	defer func() {
		// Only mark inactive if the context was NOT cancelled (natural exit).
		// When we cancel the context (Stop/Pause), restartWithOffset/Resume
		// manage the active flag directly — we must not overwrite it.
		if ctx.Err() == nil {
			st.active.Store(false)
		}
	}()

	args := st.buildFFmpegArgs()
	slog.Debug("ffmpeg command", "args", args)
	slog.Info("Starting ffmpeg", "session_id", st.sessionID)

	st.cmd = exec.CommandContext(ctx, "ffmpeg", args...)
	stdout, err := st.cmd.StdoutPipe()
	if err != nil {
		st.err = err
		slog.Error("Failed to create ffmpeg stdout pipe", "error", err)
		close(st.started)
		return
	}
	stderr, err := st.cmd.StderrPipe()
	if err != nil {
		st.err = err
		slog.Error("Failed to create ffmpeg stderr pipe", "error", err)
		close(st.started)
		return
	}

	if err := st.cmd.Start(); err != nil {
		st.err = err
		slog.Error("Failed to start ffmpeg", "error", err)
		close(st.started)
		return
	}

	slog.Info("ffmpeg started", "session_id", st.sessionID, "pid", st.cmd.Process.Pid)
	close(st.started)

	go st.readProgress(stderr)

	buf := make([]byte, 65536)
	for {
		n, readErr := stdout.Read(buf)
		if n > 0 {
			st.ringBuf.Write(buf[:n])
			st.broadcast(buf[:n])
		}
		if readErr != nil {
			if readErr != io.EOF {
				slog.Error("ffmpeg stdout read error", "session_id", st.sessionID, "error", readErr)
			}
			break
		}

		select {
		case <-ctx.Done():
			slog.Info("ffmpeg context cancelled", "session_id", st.sessionID)
			return
		default:
		}
	}

	st.cmd.Wait()
	slog.Info("ffmpeg exited", "session_id", st.sessionID)
}

func (st *stream) buildFFmpegArgs() []string {
	cfg := st.ffmpegCfg

	args := []string{
		"-hide_banner", "-loglevel", "warning",
	}

	if cfg.Reconnect {
		args = append(args,
			"-reconnect", "1",
			"-reconnect_streamed", "1",
			"-reconnect_delay_max", "5",
		)
	}

	if len(cfg.ExtraInputArgs) > 0 {
		args = append(args, cfg.ExtraInputArgs...)
	}

	if st.resumeOffset > 0 {
		slog.Debug("Seek offset applied", "session_id", st.sessionID, "offset", st.resumeOffset)
		args = append(args, "-ss", formatDuration(st.resumeOffset))
	}

	args = append(args, "-i", st.sourceURI)

	args = append(args, "-vn")

	if cfg.Channels > 0 {
		args = append(args, "-ac", fmt.Sprintf("%d", cfg.Channels))
	}
	if cfg.SampleRate > 0 {
		args = append(args, "-ar", fmt.Sprintf("%d", cfg.SampleRate))
	}
	if cfg.Codec != "" {
		args = append(args, "-codec:a", cfg.Codec)
	}
	if cfg.Bitrate != "" {
		args = append(args, "-b:a", cfg.Bitrate)
	}

	if len(cfg.ExtraOutputArgs) > 0 {
		args = append(args, cfg.ExtraOutputArgs...)
	}

	args = append(args, "-f", cfg.OutputFormat, "pipe:1")

	// Write progress lines to stderr so we can parse output timestamps.
	args = append(args, "-progress", "pipe:2")

	return args
}

func (st *stream) readProgress(stderr io.Reader) {
	scanner := bufio.NewScanner(stderr)
	var errLines []string
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "out_time_ms=") {
			if ms, err := strconv.ParseInt(strings.TrimPrefix(line, "out_time_ms="), 10, 64); err == nil {
				st.ffmpegTime.Store(ms * int64(time.Millisecond))
			}
		} else if strings.HasPrefix(line, "out_time=") {
			// Fallback for ffmpeg versions without out_time_ms
			if t, err := time.Parse("15:04:05.000000", strings.TrimPrefix(line, "out_time=")); err == nil {
				ns := int64(t.Hour())*int64(time.Hour) +
					int64(t.Minute())*int64(time.Minute) +
					int64(t.Second())*int64(time.Second) +
					int64(t.Nanosecond())
				st.ffmpegTime.Store(ns)
			}
		} else if !isProgressLine(line) {
			errLines = append(errLines, line)
			if len(errLines) > 10 {
				errLines = errLines[len(errLines)-10:]
			}
		}
	}
	if len(errLines) > 0 {
		slog.Warn("ffmpeg stderr", "session_id", st.sessionID, "output", strings.Join(errLines, "\n"))
	}
}

var progressKeys = []string{
	"progress=", "frame=", "fps=", "stream_", "bitrate=", "total_size=",
	"out_time=", "out_time_ms=", "out_time_us=",
	"speed=", "dup_frames=", "drop_frames=",
}

func isProgressLine(line string) bool {
	if line == "" {
		return true
	}
	for _, p := range progressKeys {
		if strings.HasPrefix(line, p) {
			return true
		}
	}
	return false
}

func formatDuration(d time.Duration) string {
	return fmt.Sprintf("%d.%03d", int64(d.Seconds()), d.Milliseconds()%1000)
}

func contentTypeForFormat(format string) string {
	switch format {
	case "mp3":
		return "audio/mpeg"
	case "opus":
		return "audio/opus"
	case "ogg":
		return "audio/ogg"
	case "flac":
		return "audio/flac"
	case "aac":
		return "audio/aac"
	case "wav":
		return "audio/wav"
	default:
		return "audio/" + format
	}
}

func (st *stream) broadcast(data []byte) {
	if !st.active.Load() {
		return
	}
	st.clientsMu.Lock()
	defer st.clientsMu.Unlock()

	for id, cw := range st.clients {
		select {
		case <-cw.ctx.Done():
			delete(st.clients, id)
		default:
			_, err := cw.w.Write(data)
			if err != nil {
				slog.Debug("Write error to client, disconnecting", "client_id", id, "error", err)
				cw.cancel()
				delete(st.clients, id)
			} else {
				if cw.flusher != nil {
					cw.flusher.Flush()
				}
			}
		}
	}
}

func generateClientID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func extractSessionID(path string) string {
	dot := strings.LastIndexByte(path, '.')
	if dot < 0 {
		return path
	}
	return path[:dot]
}
