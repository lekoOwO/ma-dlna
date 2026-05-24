package stream

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/leko/ma-dlna/internal/config"
)

type TokenValidator func(sessionID, token string) bool

type Streamer struct {
	cfg            *config.Config
	mu             sync.Mutex
	streams        map[string]*stream
	tokenValidator TokenValidator
}

type stream struct {
	sessionID  string
	sourceURI  string
	ringBuf    *RingBuffer
	cmd        *exec.Cmd
	cancel     context.CancelFunc
	active     atomic.Bool
	clients    map[string]*clientWriter
	clientsMu  sync.Mutex
	started    chan struct{}
	err        error
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
	}
	st.active.Store(true)
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

func (s *Streamer) IsRunning(sessionID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.streams[sessionID]; ok {
		return st.active.Load()
	}
	return false
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
	parts := strings.SplitN(path, ".mp3", 2)
	if len(parts) == 0 {
		http.Error(w, "Invalid stream path", http.StatusBadRequest)
		return
	}
	sessionID := parts[0]
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

	if r.Method == http.MethodHead {
		w.Header().Set("Content-Type", "audio/mpeg")
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
	st.clientsMu.Unlock()

	slog.Info("Client attached to stream", "session_id", sessionID, "client_id", clientID)

	w.Header().Set("Content-Type", "audio/mpeg")
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
	defer st.active.Store(false)

	args := st.buildFFmpegArgs()
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

	go func() {
		limited := io.LimitReader(stderr, 4096)
		data, _ := io.ReadAll(limited)
		if len(data) > 0 {
			slog.Warn("ffmpeg stderr", "session_id", st.sessionID, "output", string(data))
		}
	}()

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
	args := []string{
		"-hide_banner", "-loglevel", "warning",
	}

	args = append(args,
		"-reconnect", "1",
		"-reconnect_streamed", "1",
		"-reconnect_delay_max", "5",
	)

	args = append(args, "-i", st.sourceURI)

	args = append(args,
		"-vn",
		"-ac", "2",
		"-ar", "44100",
		"-codec:a", "libmp3lame",
		"-b:a", "192k",
		"-f", "mp3",
		"pipe:1",
	)

	return args
}

func (st *stream) broadcast(data []byte) {
	st.clientsMu.Lock()
	defer st.clientsMu.Unlock()

	for id, cw := range st.clients {
		select {
		case <-cw.ctx.Done():
			continue
		default:
			_, err := cw.w.Write(data)
			if err != nil {
				slog.Debug("Write error to client, disconnecting", "client_id", id, "error", err)
				cw.cancel()
				continue
			}
			if cw.flusher != nil {
				cw.flusher.Flush()
			}
		}
	}
}

func generateClientID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}
