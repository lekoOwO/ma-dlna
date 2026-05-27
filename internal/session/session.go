package session

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"log/slog"
	neturl "net/url"
	"strings"
	"sync"
	"time"

	"github.com/leko/ma-dlna/internal/config"
	"github.com/leko/ma-dlna/internal/stream"
)

type State string

const (
	StateIdle     State = "idle"
	StateLoaded   State = "loaded"
	StateStarting State = "starting"
	StatePlaying  State = "playing"
	StatePaused   State = "paused"
	StateStopped  State = "stopped"
	StateError    State = "error"
)

type Metadata struct {
	Title       string `json:"title"`
	Artist      string `json:"artist"`
	Album       string `json:"album"`
	AlbumArtURI string `json:"album_art_uri"`
	Duration    string `json:"duration"`
	ContentType string `json:"content_type"`
}

type Session struct {
	ID          string    `json:"session_id"`
	SourceURI   string    `json:"source_uri"`
	MetadataRaw string    `json:"metadata_raw"`
	Metadata    *Metadata `json:"metadata_parsed"`
	State       State     `json:"state"`
	StreamURL   string    `json:"stream_url"`
	StreamToken string    `json:"-"`
	NextURI     string    `json:"-"`
	PlayMode    string    `json:"-"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	Error       string    `json:"error,omitempty"`
}

const maxSessions = 64

type ErrorNotifier func(sessionID string, err error)

type Manager struct {
	mu               sync.RWMutex
	sessions         map[string]*Session
	sessionGenID     map[string]uint64
	currentSessionID string
	cfg              *config.Config
	streamer         *stream.Streamer
	errorNotifier    ErrorNotifier
}

func NewManager(cfg *config.Config, streamer *stream.Streamer) *Manager {
	m := &Manager{
		sessions:     make(map[string]*Session),
		sessionGenID: make(map[string]uint64),
		cfg:          cfg,
		streamer:     streamer,
	}
	if streamer != nil {
		streamer.SetGenStartCallback(func(sessionID string, genID uint64) {
			m.SetSessionGenID(sessionID, genID)
		})
	}
	return m
}

func (m *Manager) SetErrorNotifier(n ErrorNotifier) {
	m.errorNotifier = n
}

// SetSessionGenID records the expected generation ID for a session.
// Callbacks for old generations are rejected by VerifyGenID.
func (m *Manager) SetSessionGenID(sessionID string, genID uint64) {
	m.mu.Lock()
	m.sessionGenID[sessionID] = genID
	m.mu.Unlock()
}

// VerifyGenID returns true if genID matches the expected generation for this session.
func (m *Manager) VerifyGenID(sessionID string, genID uint64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sessionGenID[sessionID] == genID
}

// SetCurrentSessionID is called when a new AVTransport session replaces the current one.
func (m *Manager) SetCurrentSessionID(id string) {
	m.mu.Lock()
	m.currentSessionID = id
	m.mu.Unlock()
}

// CurrentSessionID returns the ID of the current AVTransport session.
func (m *Manager) CurrentSessionID() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.currentSessionID
}

// CurrentSession returns the current session identified by currentSessionID,
// falling back to ActiveSession() for backward compatibility.
func (m *Manager) CurrentSession() *Session {
	m.mu.RLock()
	sid := m.currentSessionID
	m.mu.RUnlock()
	if sid != "" {
		if s := m.Get(sid); s != nil {
			return s
		}
	}
	return nil
}

func (m *Manager) Create(sourceURI, metadataXML string) *Session {
	return m.CreateWithBase(sourceURI, metadataXML, m.cfg.Server.PublicBaseURL)
}

func (m *Manager) CreateWithBase(sourceURI, metadataXML, baseURL string) *Session {
	var toStop []string

	m.mu.Lock()
	for _, s := range m.sessions {
		if s.State == StatePlaying || s.State == StateStarting || s.State == StatePaused || s.State == StateLoaded || s.State == StateError {
			slog.Info("Stopping existing active session", "session_id", s.ID)
			toStop = append(toStop, s.ID)
			s.State = StateStopped
			s.UpdatedAt = time.Now()
		}
	}

	if len(m.sessions) >= maxSessions {
		m.evictLocked()
	}

	id := generateID()
	token := generateToken()
	parsed := parseDIDL(metadataXML)
	resolveMetadataURIs(parsed, sourceURI)
	slog.Info("DIDL metadata parsed", "title", parsed.Title, "artist", parsed.Artist, "duration", parsed.Duration, "content_type", parsed.ContentType)

	s := &Session{
		ID:          id,
		SourceURI:   sourceURI,
		MetadataRaw: metadataXML,
		Metadata:    parsed,
		State:       StateLoaded,
		StreamToken: token,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	ext := m.cfg.FFmpeg.OutputFormat
	if ext == "opus" {
		ext = "ogg"
	}
	s.StreamURL = baseURL + "/live/" + id + "." + ext + "?token=" + token

	m.sessions[id] = s
	m.currentSessionID = id
	m.mu.Unlock()

	for _, sid := range toStop {
		m.streamer.Stop(sid)
	}

	slog.Info("Session created", "session_id", id, "source", safeURL(sourceURI), "stream_url", safeURL(s.StreamURL))
	return s
}

func (m *Manager) Play(sessionID string) error {
	m.mu.Lock()
	s, ok := m.sessions[sessionID]
	if !ok {
		m.mu.Unlock()
		return ErrNotFound
	}

	if s.State == StatePaused {
		s.State = StateStarting
		s.UpdatedAt = time.Now()
		m.mu.Unlock()
		m.streamer.Resume(sessionID)
		slog.Info("Session resuming from pause", "session_id", sessionID)
		return nil
	}

	if s.State == StateError {
		s.State = StateStarting
		s.Error = ""
		s.UpdatedAt = time.Now()
		m.mu.Unlock()
		slog.Info("Session retrying from error", "session_id", sessionID)
		return nil
	}

	if s.State != StateLoaded && s.State != StateStopped {
		m.mu.Unlock()
		return nil
	}

	s.State = StateStarting
	s.UpdatedAt = time.Now()
	m.mu.Unlock()

	slog.Info("Session play requested", "session_id", sessionID)
	return nil
}

func (m *Manager) Stop(sessionID string) error {
	m.mu.Lock()
	s, ok := m.sessions[sessionID]
	if !ok {
		m.mu.Unlock()
		return ErrNotFound
	}
	s.State = StateStopped
	s.UpdatedAt = time.Now()
	m.mu.Unlock()

	m.streamer.Stop(sessionID)
	slog.Info("Session stopped", "session_id", sessionID)
	return nil
}

// StopIfGeneration verifies genID, cleans up the matching stream generation,
// then re-verifies before marking the session stopped.
func (m *Manager) StopIfGeneration(sessionID string, genID uint64) bool {
	m.mu.Lock()
	s, ok := m.sessions[sessionID]
	if !ok {
		m.mu.Unlock()
		return false
	}
	if genID != 0 && m.sessionGenID[sessionID] != genID {
		m.mu.Unlock()
		return false
	}
	if s.State == StateError {
		m.mu.Unlock()
		return false
	}
	m.mu.Unlock()

	if !m.streamer.StopIfGeneration(sessionID, genID) {
		return false
	}

	m.mu.Lock()
	s, ok = m.sessions[sessionID]
	if !ok {
		m.mu.Unlock()
		return false
	}
	if genID != 0 && m.sessionGenID[sessionID] != genID {
		m.mu.Unlock()
		return false
	}
	if s.State == StateError {
		m.mu.Unlock()
		return false
	}
	s.State = StateStopped
	s.UpdatedAt = time.Now()
	m.mu.Unlock()

	slog.Info("Session stopped with generation check", "session_id", sessionID)
	return true
}

// MarkStopped updates the session state to stopped without touching the streamer.
// Used for natural EOF callbacks where the stream has already exited and calling
// Streamer.Stop() would race with a new stream for the same session ID.
func (m *Manager) MarkStopped(sessionID string) {
	m.MarkStoppedIfGeneration(sessionID, 0)
}

// MarkStoppedIfGeneration atomically verifies genID and updates state to stopped.
// genID=0 skips the generation check. Unlike StopIfGeneration, this does not
// touch the streamer — used where the stream has already exited (EOF).
func (m *Manager) MarkStoppedIfGeneration(sessionID string, genID uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.sessions[sessionID]; ok {
		if genID != 0 && m.sessionGenID[sessionID] != genID {
			return false
		}
		if s.State == StateError {
			return false
		}
		s.State = StateStopped
		s.UpdatedAt = time.Now()
		slog.Info("Session marked stopped", "session_id", sessionID)
		return true
	}
	return false
}

func (m *Manager) Pause(sessionID string) error {
	m.mu.Lock()
	s, ok := m.sessions[sessionID]
	if !ok {
		m.mu.Unlock()
		return ErrNotFound
	}
	s.State = StatePaused
	s.UpdatedAt = time.Now()
	m.mu.Unlock()

	elapsed := m.streamer.Pause(sessionID)
	slog.Info("Session paused", "session_id", sessionID, "position", elapsed.Round(time.Second))
	return nil
}

func (m *Manager) SetPlaying(sessionID string) {
	m.SetPlayingAccepted(sessionID)
}

// SetPlayingAccepted transitions the session from Starting to Playing and returns
// true if the transition was valid. Used by first-client callback to know whether
// to fire the PLAYING event.
func (m *Manager) SetPlayingAccepted(sessionID string) bool {
	return m.SetPlayingAcceptedIfGeneration(sessionID, 0)
}

// SetPlayingAcceptedIfGeneration transitions the session from Starting to Playing
// only if genID matches the expected generation. It returns true on success.
// genID=0 skips the generation check.
func (m *Manager) SetPlayingAcceptedIfGeneration(sessionID string, genID uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.sessions[sessionID]; ok {
		if s.State != StateStarting {
			return false
		}
		if genID != 0 && m.sessionGenID[sessionID] != genID {
			return false
		}
		s.State = StatePlaying
		s.UpdatedAt = time.Now()
		return true
	}
	return false
}

func (m *Manager) MarkPlayingIfGeneration(sessionID string, genID uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.sessions[sessionID]; ok {
		if genID != 0 && m.sessionGenID[sessionID] != genID {
			return false
		}
		if s.State == StatePlaying || s.State == StateStopped || s.State == StateError {
			return false
		}
		s.State = StatePlaying
		s.UpdatedAt = time.Now()
		return true
	}
	return false
}

func (m *Manager) SetError(sessionID string, errMsg string) {
	m.SetErrorIfGeneration(sessionID, 0, errMsg)
}

// SetErrorIfGeneration sets the session to error state only if genID matches the
// expected generation (nonzero). genID=0 skips the generation check.
// It returns true if the error was actually set.
func (m *Manager) SetErrorIfGeneration(sessionID string, genID uint64, errMsg string) bool {
	m.mu.Lock()
	s, ok := m.sessions[sessionID]
	if !ok {
		m.mu.Unlock()
		return false
	}
	if genID != 0 && m.sessionGenID[sessionID] != genID {
		m.mu.Unlock()
		return false
	}
	s.State = StateError
	s.Error = errMsg
	s.UpdatedAt = time.Now()
	n := m.errorNotifier
	m.mu.Unlock()

	if n != nil {
		n(sessionID, fmt.Errorf("%s", errMsg))
	}
	return true
}

// SetErrorIfNoGeneration sets the session to error state only if the session
// has no generation recorded (sessionGenID[sessionID] == 0). It returns true
// if the error was actually set. Used when an operation requires an active
// stream generation to exist.
func (m *Manager) SetErrorIfNoGeneration(sessionID string, errMsg string) bool {
	m.mu.Lock()
	s, ok := m.sessions[sessionID]
	if !ok {
		m.mu.Unlock()
		return false
	}
	if m.sessionGenID[sessionID] != 0 {
		m.mu.Unlock()
		return false
	}
	s.State = StateError
	s.Error = errMsg
	s.UpdatedAt = time.Now()
	n := m.errorNotifier
	m.mu.Unlock()

	if n != nil {
		n(sessionID, fmt.Errorf("%s", errMsg))
	}
	return true
}

// MarkPausedIfGeneration transitions StatePlaying or StateStarting to StatePaused
// only if genID matches the expected generation (nonzero). genID=0 skips the check.
// It returns true if the state was actually changed. Does not touch streamer.Pause.
func (m *Manager) MarkPausedIfGeneration(sessionID string, genID uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.sessions[sessionID]; ok {
		if genID != 0 && m.sessionGenID[sessionID] != genID {
			return false
		}
		if s.State != StatePlaying && s.State != StateStarting {
			return false
		}
		s.State = StatePaused
		s.UpdatedAt = time.Now()
		return true
	}
	return false
}

// IsCurrentGenerationActive returns true only if sessionID is the current
// session, its state is StateStarting or StatePlaying, and genID matches
// the expected generation (when nonzero). genID=0 skips the generation check.
func (m *Manager) IsCurrentGenerationActive(sessionID string, genID uint64) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.isCurrentGenerationActiveLocked(sessionID, genID)
}

func (m *Manager) IsCurrentGenerationState(sessionID string, genID uint64, state State) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if !m.isCurrentGenerationActiveLocked(sessionID, genID) {
		return false
	}
	s, ok := m.sessions[sessionID]
	return ok && s.State == state
}

func (m *Manager) isCurrentGenerationActiveLocked(sessionID string, genID uint64) bool {
	if m.currentSessionID != sessionID {
		return false
	}

	s, ok := m.sessions[sessionID]
	if !ok {
		return false
	}

	if s.State != StateStarting && s.State != StatePlaying {
		return false
	}

	if genID != 0 && m.sessionGenID[sessionID] != genID {
		return false
	}

	return true
}

func (m *Manager) MarkPlaybackAcceptedIfGeneration(sessionID string, genID uint64) bool {
	m.mu.RLock()
	if !m.isCurrentGenerationActiveLocked(sessionID, genID) {
		m.mu.RUnlock()
		return false
	}
	m.mu.RUnlock()

	if !m.streamer.MarkPlaybackAcceptedIfGeneration(sessionID, genID) {
		return false
	}

	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.isCurrentGenerationActiveLocked(sessionID, genID)
}

// AcceptPlaybackIfGeneration accepts the current generation as real playback
// after the downstream play_media call succeeds, then transitions Starting to
// Playing. It returns true only for the state transition that should emit a
// PLAYING event.
func (m *Manager) AcceptPlaybackIfGeneration(sessionID string, genID uint64) bool {
	m.mu.RLock()
	if !m.isCurrentGenerationActiveLocked(sessionID, genID) {
		m.mu.RUnlock()
		return false
	}
	m.mu.RUnlock()

	if !m.streamer.MarkPlaybackAcceptedIfGeneration(sessionID, genID) {
		return false
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.isCurrentGenerationActiveLocked(sessionID, genID) {
		return false
	}
	s := m.sessions[sessionID]
	if s.State != StateStarting {
		return false
	}
	s.State = StatePlaying
	s.UpdatedAt = time.Now()
	return true
}

// StopWithErrorIfGenerationActive stops the matching stream generation and sets
// ERROR only if the session is still current and actively starting/playing.
func (m *Manager) StopWithErrorIfGenerationActive(sessionID string, genID uint64, errMsg string) bool {
	m.mu.RLock()
	if !m.isCurrentGenerationActiveLocked(sessionID, genID) {
		m.mu.RUnlock()
		return false
	}
	m.mu.RUnlock()

	if !m.streamer.StopIfGeneration(sessionID, genID) {
		return false
	}

	m.mu.Lock()
	s, ok := m.sessions[sessionID]
	if !ok || !m.isCurrentGenerationActiveLocked(sessionID, genID) {
		m.mu.Unlock()
		return false
	}
	s.State = StateError
	s.Error = errMsg
	s.UpdatedAt = time.Now()
	n := m.errorNotifier
	m.mu.Unlock()

	if n != nil {
		n(sessionID, fmt.Errorf("%s", errMsg))
	}
	return true
}

// CurrentGenID returns the expected generation ID for a session.
func (m *Manager) CurrentGenID(sessionID string) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sessionGenID[sessionID]
}

func (m *Manager) Get(sessionID string) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if s, ok := m.sessions[sessionID]; ok {
		return s.snapshot()
	}
	return nil
}

func (m *Manager) ActiveSession() *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, s := range m.sessions {
		if s.State == StatePlaying || s.State == StateStarting || s.State == StateLoaded || s.State == StatePaused {
			return s.snapshot()
		}
	}
	return nil
}

// StatusSession returns the active session for status reporting,
// including StateError sessions that ActiveSession excludes.
func (m *Manager) StatusSession() *Session {
	if s := m.CurrentSession(); s != nil {
		return s
	}
	return m.ActiveSession()
}

func (m *Manager) Count() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sessions)
}

func (m *Manager) AllSessions() []*Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		result = append(result, s.snapshot())
	}
	return result
}

func (s *Session) snapshot() *Session {
	copy := *s
	if s.Metadata != nil {
		m := *s.Metadata
		copy.Metadata = &m
	}
	return &copy
}

func (m *Manager) StartStream(sessionID, sourceURI string) {
	m.streamer.Start(sessionID, sourceURI)
}

func (m *Manager) Elapsed(sessionID string) time.Duration {
	return m.streamer.Elapsed(sessionID)
}

func (m *Manager) Seek(sessionID string, offset time.Duration) {
	m.mu.Lock()
	if s, ok := m.sessions[sessionID]; ok {
		if s.State == StatePlaying || s.State == StateStarting {
			s.State = StateStarting
			s.UpdatedAt = time.Now()
			m.sessionGenID[sessionID] = 0
		}
	}
	m.mu.Unlock()
	m.streamer.Seek(sessionID, offset)
}

func (m *Manager) Resume(sessionID string) {
	m.streamer.Resume(sessionID)
}

func (m *Manager) SetNextURI(sessionID, uri string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.sessions[sessionID]; ok {
		s.NextURI = uri
	}
}

func (m *Manager) SetPlayMode(sessionID, mode string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.sessions[sessionID]; ok {
		s.PlayMode = mode
	}
}

func (m *Manager) ValidateToken(sessionID, token string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.sessions[sessionID]
	if !ok {
		return false
	}
	if m.cfg.Security.RequireStreamToken && s.StreamToken != token {
		return false
	}
	return true
}

func (m *Manager) evictLocked() {
	// Evict in order: stopped/error → paused. Never evict active sessions.
	evictByState := func(states ...State) bool {
		var oldest *Session
		for _, s := range m.sessions {
			if s.ID == m.currentSessionID {
				continue
			}
			for _, st := range states {
				if s.State == st {
					if oldest == nil || s.UpdatedAt.Before(oldest.UpdatedAt) {
						oldest = s
					}
				}
			}
		}
		if oldest != nil && oldest.ID != m.currentSessionID {
			delete(m.sessions, oldest.ID)
			slog.Debug("Evicted session", "session_id", oldest.ID, "state", string(oldest.State))
			return true
		}
		return false
	}

	// Try stopped and error sessions first
	for evictByState(StateStopped, StateError) {
		if len(m.sessions) < maxSessions {
			return
		}
	}
	// Then paused
	for evictByState(StatePaused) {
		if len(m.sessions) < maxSessions {
			return
		}
	}
}

func (m *Manager) Shutdown() {
	m.mu.Lock()
	ids := make([]string, 0, len(m.sessions))
	for id := range m.sessions {
		ids = append(ids, id)
	}
	m.mu.Unlock()

	for _, id := range ids {
		m.streamer.Stop(id)
	}
}

func generateID() string {
	b := make([]byte, 12)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func generateToken() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// parseDIDL extracts metadata from DIDL-Lite XML, including <res duration="...">.
func parseDIDL(xmlStr string) *Metadata {
	if xmlStr == "" {
		return &Metadata{}
	}

	// DIDL-Lite element hierarchy:
	//   <DIDL-Lite>
	//     <item>   <title/>  <creator/>  <artist/>  <album/>  <albumArtURI/>   </item>
	//   </DIDL-Lite>
	type didlRes struct {
		Duration     string `xml:"duration,attr"`
		ProtocolInfo string `xml:"protocolInfo,attr"`
		URI          string `xml:",chardata"`
	}

	type didlItem struct {
		XMLName     xml.Name `xml:"item"`
		Title       string   `xml:"title"`
		Creator     string   `xml:"creator"`
		Artist      string   `xml:"artist"`
		Album       string   `xml:"album"`
		AlbumArtURI string   `xml:"albumArtURI"`
		Res         didlRes  `xml:"res"`
	}

	// Try with DIDL-Lite wrapper (full UPnP format)
	type didlDoc struct {
		XMLName xml.Name   `xml:"DIDL-Lite"`
		Items   []didlItem `xml:"item"`
	}
	var doc didlDoc
	if err := xml.Unmarshal([]byte(xmlStr), &doc); err == nil && len(doc.Items) > 0 {
		it := doc.Items[0]
		return buildMetadata(it.Title, it.Creator, it.Artist, it.Album, it.AlbumArtURI, it.Res.Duration, contentTypeFromProtocolInfo(it.Res.ProtocolInfo))
	}

	// Fallback: bare <item>
	var it didlItem
	if err := xml.Unmarshal([]byte(xmlStr), &it); err == nil {
		return buildMetadata(it.Title, it.Creator, it.Artist, it.Album, it.AlbumArtURI, it.Res.Duration, contentTypeFromProtocolInfo(it.Res.ProtocolInfo))
	}

	return &Metadata{}
}

func buildMetadata(title, creator, artist, album, albumArtURI, duration, contentType string) *Metadata {
	md := &Metadata{
		Title:       title,
		Album:       album,
		AlbumArtURI: albumArtURI,
		Duration:    duration,
		ContentType: contentType,
	}
	if artist != "" {
		md.Artist = artist
	} else if creator != "" {
		md.Artist = creator
	}
	return md
}

func contentTypeFromProtocolInfo(protocolInfo string) string {
	parts := strings.Split(protocolInfo, ":")
	if len(parts) < 3 {
		return ""
	}
	if parts[2] == "*" {
		return ""
	}
	return parts[2]
}

func resolveMetadataURIs(md *Metadata, sourceURI string) {
	if md == nil || md.AlbumArtURI == "" {
		return
	}
	art, err := neturl.Parse(md.AlbumArtURI)
	if err != nil || art.IsAbs() {
		return
	}
	base, err := neturl.Parse(sourceURI)
	if err != nil || base.Scheme == "" || base.Host == "" {
		return
	}
	md.AlbumArtURI = base.ResolveReference(art).String()
}

var ErrNotFound = errors.New("session not found")

func safeURL(raw string) string {
	if i := strings.Index(raw, "://"); i > 0 {
		if j := strings.Index(raw[i+3:], "@"); j > 0 {
			raw = raw[:i+3] + "***@" + raw[i+3+j+1:]
		}
	}
	if i := strings.IndexByte(raw, '?'); i >= 0 {
		raw = raw[:i] + "?..."
	}
	return raw
}
