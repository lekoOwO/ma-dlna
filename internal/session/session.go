package session

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"errors"
	"fmt"
	"log/slog"
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
	currentSessionID string
	cfg              *config.Config
	streamer         *stream.Streamer
	errorNotifier    ErrorNotifier
}

func NewManager(cfg *config.Config, streamer *stream.Streamer) *Manager {
	return &Manager{
		sessions: make(map[string]*Session),
		cfg:      cfg,
		streamer: streamer,
	}
}

func (m *Manager) SetErrorNotifier(n ErrorNotifier) {
	m.errorNotifier = n
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
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.sessions[sessionID]; ok {
		if s.State != StateStarting {
			return
		}
		s.State = StatePlaying
		s.UpdatedAt = time.Now()
	}
}

func (m *Manager) SetError(sessionID string, errMsg string) {
	m.mu.Lock()
	if s, ok := m.sessions[sessionID]; ok {
		s.State = StateError
		s.Error = errMsg
		s.UpdatedAt = time.Now()
	}
	n := m.errorNotifier
	m.mu.Unlock()

	if n != nil {
		n(sessionID, fmt.Errorf("%s", errMsg))
	}
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

func parseDIDL(xmlStr string) *Metadata {
	if xmlStr == "" {
		return &Metadata{}
	}

	// DIDL-Lite element hierarchy:
	//   <DIDL-Lite>
	//     <item>   <title/>  <creator/>  <artist/>  <album/>  <albumArtURI/>   </item>
	//   </DIDL-Lite>
	type didlItem struct {
		XMLName     xml.Name `xml:"item"`
		Title       string   `xml:"title"`
		Creator     string   `xml:"creator"`
		Artist      string   `xml:"artist"`
		Album       string   `xml:"album"`
		AlbumArtURI string   `xml:"albumArtURI"`
	}

	// Try with DIDL-Lite wrapper (full UPnP format)
	type didlDoc struct {
		XMLName xml.Name   `xml:"DIDL-Lite"`
		Items   []didlItem `xml:"item"`
	}
	var doc didlDoc
	if err := xml.Unmarshal([]byte(xmlStr), &doc); err == nil && len(doc.Items) > 0 {
		it := doc.Items[0]
		return buildMetadata(it.Title, it.Creator, it.Artist, it.Album, it.AlbumArtURI)
	}

	// Fallback: bare <item>
	var it didlItem
	if err := xml.Unmarshal([]byte(xmlStr), &it); err == nil {
		return buildMetadata(it.Title, it.Creator, it.Artist, it.Album, it.AlbumArtURI)
	}

	return &Metadata{}
}

func buildMetadata(title, creator, artist, album, albumArtURI string) *Metadata {
	md := &Metadata{
		Title:       title,
		Album:       album,
		AlbumArtURI: albumArtURI,
	}
	if artist != "" {
		md.Artist = artist
	} else if creator != "" {
		md.Artist = creator
	}
	return md
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
