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

type ErrorCallback func(sessionID string, err error)

type Streamer struct {
	cfg            *config.Config
	mu             sync.Mutex
	streams        map[string]*stream
	tokenValidator TokenValidator
	firstClientCB  FirstClientCallback
	errorCB        ErrorCallback
	endCB          EndCallback
}

type EndCallback func(sessionID string)

type stream struct {
	sessionID    string
	sourceURI    string
	active       atomic.Bool
	clientsMu    sync.Mutex
	ffmpegCfg    config.FFmpegConfig
	startTime    time.Time
	resumeOffset time.Duration
	runsInFlight atomic.Int32
	genMu        sync.Mutex
	errorCB      ErrorCallback
	endCB        EndCallback

	gen *streamGeneration
}

type clientWriter struct {
	id      string
	w       http.ResponseWriter
	flusher http.Flusher
	ctx     context.Context
	cancel  context.CancelFunc
	ch      chan []byte
}

// streamGeneration holds the per-run mutable state for a single ffmpeg instance.
// It is replaced on each Pause/Seek/Resume cycle.
type streamGeneration struct {
	mu         sync.Mutex
	ctx        context.Context
	cancel     context.CancelFunc
	started    chan struct{}
	done       chan struct{}
	ringBuf    *RingBuffer
	cmd        *exec.Cmd
	clients    map[string]*clientWriter
	offset     time.Duration
	err        error
	ffmpegTime atomic.Int64
}

func (st *stream) currentGen() *streamGeneration {
	st.genMu.Lock()
	defer st.genMu.Unlock()
	return st.gen
}

func (gen *streamGeneration) getErr() error {
	gen.mu.Lock()
	defer gen.mu.Unlock()
	return gen.err
}

func (gen *streamGeneration) setErr(err error) {
	gen.mu.Lock()
	defer gen.mu.Unlock()
	gen.err = err
}

func (gen *streamGeneration) killCmd() {
	gen.mu.Lock()
	defer gen.mu.Unlock()
	if gen.cmd != nil && gen.cmd.Process != nil {
		gen.cmd.Process.Kill()
	}
}

func (gen *streamGeneration) getCmd() *exec.Cmd {
	gen.mu.Lock()
	defer gen.mu.Unlock()
	return gen.cmd
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
		ffmpegCfg: s.cfg.FFmpeg,
		errorCB:   s.errorCB,
		endCB:     s.endCB,
		gen: &streamGeneration{
			ctx:     ctx,
			cancel:  cancel,
			started: make(chan struct{}),
			done:    make(chan struct{}),
			ringBuf: NewRingBuffer(s.cfg.Stream.RingBufferBytes),
			clients: make(map[string]*clientWriter),
		},
	}
	st.active.Store(true)
	st.runsInFlight.Store(1)
	st.startTime = time.Now()
	st.resumeOffset = 0
	s.streams[sessionID] = st

	go st.run(st.gen)
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
		st.genMu.Lock()
		gen := st.gen
		st.genMu.Unlock()
		if gen != nil {
			gen.cancel()
			gen.killCmd()
			st.closeClients(gen)
		}
		slog.Info("Stream stopped", "session_id", sessionID)
	}
}

func (st *stream) closeClients(gen *streamGeneration) {
	st.clientsMu.Lock()
	for _, c := range gen.clients {
		c.cancel()
	}
	gen.clients = make(map[string]*clientWriter)
	st.clientsMu.Unlock()
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
	st.genMu.Lock()
	oldGen := st.gen
	st.genMu.Unlock()
	if st.active.Swap(false) && oldGen != nil {
		oldGen.cancel()
		oldGen.killCmd()
		st.closeClients(oldGen)
	}
	if st.runsInFlight.Load() > 0 && oldGen != nil {
		<-oldGen.done
	}
	st.runsInFlight.Store(0)
	ctx, cancel := context.WithCancel(context.Background())
	ringBufSize := s.cfg.Stream.RingBufferBytes
	if oldGen != nil {
		ringBufSize = oldGen.ringBuf.Size()
	}
	newGen := &streamGeneration{
		ctx:     ctx,
		cancel:  cancel,
		started: make(chan struct{}),
		done:    make(chan struct{}),
		ringBuf: NewRingBuffer(ringBufSize),
		clients: make(map[string]*clientWriter),
		offset:  offset,
	}
	st.genMu.Lock()
	st.gen = newGen
	st.active.Store(true)
	st.genMu.Unlock()
}

func (s *Streamer) Resume(sessionID string) {
	s.mu.Lock()
	st, ok := s.streams[sessionID]
	s.mu.Unlock()
	if !ok || !st.active.Load() {
		return
	}
	if !st.runsInFlight.CompareAndSwap(0, 1) {
		return
	}
	st.genMu.Lock()
	resumeCtx, resumeCancel := context.WithCancel(context.Background())
	newGen := &streamGeneration{
		ctx:     resumeCtx,
		cancel:  resumeCancel,
		started: make(chan struct{}),
		done:    make(chan struct{}),
		ringBuf: NewRingBuffer(st.gen.ringBuf.Size()),
		clients: make(map[string]*clientWriter),
		offset:  st.gen.offset,
	}
	st.gen = newGen
	st.startTime = time.Now()
	st.genMu.Unlock()
	go st.run(newGen)
	slog.Info("Stream resuming", "session_id", sessionID, "offset", newGen.offset.Round(time.Second))
}

func (s *Streamer) Elapsed(sessionID string) time.Duration {
	s.mu.Lock()
	st, ok := s.streams[sessionID]
	s.mu.Unlock()
	if !ok {
		return 0
	}
	gen := st.currentGen()
	var resumeOff time.Duration
	if gen != nil {
		resumeOff = gen.offset
		ft := gen.ffmpegTime.Load()
		if ft > 0 {
			return resumeOff + time.Duration(ft)
		}
	} else {
		resumeOff = st.resumeOffset
	}
	if st.startTime.IsZero() {
		return 0
	}
	return resumeOff + time.Since(st.startTime)

}

func (s *Streamer) IsRunning(sessionID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.streams[sessionID]; ok {
		return st.active.Load() && st.runsInFlight.Load() > 0
	}
	return false
}

func (s *Streamer) SetFirstClientCallback(cb FirstClientCallback) {
	s.firstClientCB = cb
}

func (s *Streamer) SetEndCallback(cb EndCallback) {
	s.endCB = cb
}

func (s *Streamer) SetErrorCallback(cb ErrorCallback) {
	s.errorCB = cb
}

func (s *Streamer) TotalClients() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, st := range s.streams {
		st.clientsMu.Lock()
		if gen := st.currentGen(); gen != nil {
			total += len(gen.clients)
		}
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

	gen := st.currentGen()
	if gen == nil {
		http.Error(w, "Stream not available", http.StatusNotFound)
		return
	}

	select {
	case <-gen.started:
	case <-time.After(time.Duration(s.cfg.Stream.StartupTimeoutSeconds) * time.Second):
		http.Error(w, "Stream startup timeout", http.StatusServiceUnavailable)
		return
	}

	if gen.getErr() != nil {
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
		ch:     make(chan []byte, 64),
	}

	if f, ok := w.(http.Flusher); ok {
		cw.flusher = f
	}

	st.clientsMu.Lock()
	if st.currentGen() != gen || gen.ctx.Err() != nil {
		st.clientsMu.Unlock()
		http.Error(w, "Stream restarted, reconnect", http.StatusServiceUnavailable)
		return
	}
	if len(gen.clients) >= s.cfg.Stream.MaxClientsPerSession {
		st.clientsMu.Unlock()
		http.Error(w, "Too many clients", http.StatusTooManyRequests)
		return
	}

	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Accept-Ranges", "none")
	w.WriteHeader(http.StatusOK)

	if cw.flusher != nil {
		cw.flusher.Flush()
	}

	wp := gen.ringBuf.WritePosition()
	start := wp - int64(s.cfg.Stream.PrebufferBytes)
	if start < 0 {
		start = 0
	}
	prebuf := make([]byte, s.cfg.Stream.PrebufferBytes)
	n, _ := gen.ringBuf.Read(start, prebuf)
	if n > 0 {
		// Copy before enqueue so channel holds an independent slice
		cw.ch <- append([]byte(nil), prebuf[:n]...)
	}
	gen.clients[clientID] = cw
	isFirst := len(gen.clients) == 1
	st.clientsMu.Unlock()

	if isFirst && s.firstClientCB != nil {
		s.firstClientCB(sessionID)
	}

	slog.Info("Client attached to stream", "session_id", sessionID, "client_id", clientID)

	go cw.writeLoop()

	<-ctx.Done()

	st.clientsMu.Lock()
	delete(gen.clients, clientID)
	remaining := len(gen.clients)
	st.clientsMu.Unlock()

	slog.Info("Client disconnected from stream", "session_id", sessionID, "client_id", clientID, "remaining", remaining)

	if remaining == 0 {
		capturedGen := gen
		go func(snapshot *stream) {
			time.Sleep(time.Duration(s.cfg.Stream.NoClientGraceSeconds) * time.Second)
			s.mu.Lock()
			current := s.streams[sessionID]
			s.mu.Unlock()
			snapshot.clientsMu.Lock()
			currentGen := snapshot.currentGen()
			empty := len(capturedGen.clients) == 0
			snapshot.clientsMu.Unlock()
			if current == snapshot && currentGen == capturedGen && empty {
				slog.Info("No clients remaining, stopping stream", "session_id", sessionID)
				s.Stop(sessionID)
			}
		}(st)
	}
}

func (st *stream) run(gen *streamGeneration) {
	hadError := false
	defer func() {
		st.runsInFlight.Store(0)
		if gen.ctx.Err() == nil {
			st.closeClients(gen)
			st.active.Store(false)
			if !hadError && st.endCB != nil {
				st.endCB(st.sessionID)
			}
		}
		close(gen.done)
	}()

	args := st.buildFFmpegArgs(gen.offset)
	bin := st.ffmpegCfg.Binary
	if bin == "" {
		bin = "ffmpeg"
	}
	slog.Debug("ffmpeg command", "bin", bin, "arg_count", len(args))
	cmd := exec.CommandContext(gen.ctx, bin, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		hadError = true
		gen.err = err
		slog.Error("Failed to create ffmpeg stdout pipe", "error", err)
		close(gen.started)
		return
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		hadError = true
		gen.err = err
		slog.Error("Failed to create ffmpeg stderr pipe", "error", err)
		close(gen.started)
		return
	}

	if err := cmd.Start(); err != nil {
		hadError = true
		gen.err = err
		slog.Error("Failed to start ffmpeg", "error", err)
		close(gen.started)
		if st.errorCB != nil {
			st.errorCB(st.sessionID, err)
		}
		return
	}

	gen.cmd = cmd
	slog.Info("ffmpeg started", "session_id", st.sessionID, "pid", cmd.Process.Pid)
	close(gen.started)

	go st.readProgress(gen, stderr)

	buf := make([]byte, 65536)
	for {
		n, readErr := stdout.Read(buf)
		if n > 0 {
			chunk := make([]byte, n)
			copy(chunk, buf[:n])
			gen.ringBuf.Write(chunk)
			st.broadcast(gen, chunk)
		}
		if readErr != nil {
			if readErr != io.EOF {
				slog.Error("ffmpeg stdout read error", "session_id", st.sessionID, "error", readErr)
			}
			break
		}

		select {
		case <-gen.ctx.Done():
			slog.Info("ffmpeg context cancelled", "session_id", st.sessionID)
			goto waitproc
		default:
		}
	}

waitproc:
	if err := cmd.Wait(); err != nil && gen.ctx.Err() == nil {
		hadError = true
		gen.setErr(err)
		slog.Error("ffmpeg exited with error", "session_id", st.sessionID, "error", err)
		if st.errorCB != nil {
			st.errorCB(st.sessionID, err)
		}
	} else {
		slog.Info("ffmpeg exited", "session_id", st.sessionID)
	}
}

func (st *stream) buildFFmpegArgs(offset time.Duration) []string {
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

	if offset > 0 {
		slog.Debug("Seek offset applied", "session_id", st.sessionID, "offset", offset)
		args = append(args, "-ss", formatDuration(offset))
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

func (st *stream) readProgress(gen *streamGeneration, stderr io.Reader) {
	scanner := bufio.NewScanner(stderr)
	var errLines []string
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "out_time_us=") {
			if ms, err := strconv.ParseInt(strings.TrimPrefix(line, "out_time_us="), 10, 64); err == nil {
				gen.ffmpegTime.Store(ms * int64(time.Microsecond))
			}
		} else if strings.HasPrefix(line, "out_time=") {
			if t, err := time.Parse("15:04:05.000000", strings.TrimPrefix(line, "out_time=")); err == nil {
				ns := int64(t.Hour())*int64(time.Hour) +
					int64(t.Minute())*int64(time.Minute) +
					int64(t.Second())*int64(time.Second) +
					int64(t.Nanosecond())
				gen.ffmpegTime.Store(ns)
			}
		} else if !isProgressLine(line) {
			errLines = append(errLines, line)
			if len(errLines) > 10 {
				errLines = errLines[len(errLines)-10:]
			}
		}
	}
	if len(errLines) > 0 {
		slog.Warn("ffmpeg stderr", "session_id", st.sessionID, "output", redactStderr(strings.Join(errLines, "\n")))
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

func redactStderr(line string) string {
	// Redact URLs in ffmpeg stderr to avoid leaking tokens/signatures
	result := line
	for {
		i := strings.Index(result, "http")
		if i < 0 {
			break
		}
		end := i
		for end < len(result) && result[end] != ' ' && result[end] != '\n' && result[end] != ':' {
			end++
		}
		for end < len(result) && result[end] != ' ' && result[end] != '\n' {
			end++
		}
		if j := strings.IndexByte(result[i:end], '?'); j >= 0 {
			result = result[:i+j] + "?..." + result[end:]
		} else {
			break
		}
	}
	return result
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

func (st *stream) broadcast(gen *streamGeneration, data []byte) {
	if !st.active.Load() {
		return
	}
	st.clientsMu.Lock()
	for id, cw := range gen.clients {
		select {
		case <-cw.ctx.Done():
			delete(gen.clients, id)
		case cw.ch <- data:
		default:
			// client too slow, drop it
			slog.Debug("Client too slow, disconnecting", "client_id", id)
			cw.cancel()
			delete(gen.clients, id)
		}
	}
	st.clientsMu.Unlock()
}

func (cw *clientWriter) writeLoop() {
	for {
		select {
		case <-cw.ctx.Done():
			return
		case data, ok := <-cw.ch:
			if !ok {
				return
			}
			if _, err := cw.w.Write(data); err != nil {
				cw.cancel()
				return
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

func extractSessionID(path string) string {
	dot := strings.LastIndexByte(path, '.')
	if dot < 0 {
		return path
	}
	return path[:dot]
}
