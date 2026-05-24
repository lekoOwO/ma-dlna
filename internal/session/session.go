package session

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/xml"
	"errors"
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
	ID            string    `json:"session_id"`
	SourceURI     string    `json:"source_uri"`
	MetadataRaw   string    `json:"metadata_raw"`
	Metadata      *Metadata `json:"metadata_parsed"`
	State         State     `json:"state"`
	StreamURL     string    `json:"stream_url"`
	StreamToken   string    `json:"-"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	Error         string    `json:"error,omitempty"`
}

type Manager struct {
	mu       sync.RWMutex
	sessions map[string]*Session
	cfg      *config.Config
	streamer *stream.Streamer
}

func NewManager(cfg *config.Config, streamer *stream.Streamer) *Manager {
	return &Manager{
		sessions: make(map[string]*Session),
		cfg:      cfg,
		streamer: streamer,
	}
}

func (m *Manager) Create(sourceURI, metadataXML string) *Session {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, s := range m.sessions {
		if s.State == StatePlaying || s.State == StateStarting || s.State == StatePaused {
			slog.Info("Stopping existing active session", "session_id", s.ID)
			m.streamer.Stop(s.ID)
			s.State = StateStopped
			s.UpdatedAt = time.Now()
		}
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

	baseURL := m.cfg.Server.PublicBaseURL
	ext := m.cfg.FFmpeg.OutputFormat
	s.StreamURL = baseURL + "/live/" + id + "." + ext + "?token=" + token

	m.sessions[id] = s

	slog.Info("Session created", "session_id", id, "source", safeURL(sourceURI), "stream_url", s.StreamURL)
	return s
}

func (m *Manager) Play(sessionID string) error {
	m.mu.Lock()
	s, ok := m.sessions[sessionID]
	if !ok {
		m.mu.Unlock()
		return ErrNotFound
	}

	if s.State != StateLoaded && s.State != StatePaused && s.State != StateStopped {
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

	m.streamer.Stop(sessionID)
	slog.Info("Session paused", "session_id", sessionID)
	return nil
}

func (m *Manager) SetPlaying(sessionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.sessions[sessionID]; ok {
		s.State = StatePlaying
		s.UpdatedAt = time.Now()
	}
}

func (m *Manager) SetError(sessionID string, errMsg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.sessions[sessionID]; ok {
		s.State = StateError
		s.Error = errMsg
		s.UpdatedAt = time.Now()
	}
}

func (m *Manager) Get(sessionID string) *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sessions[sessionID]
}

func (m *Manager) ActiveSession() *Session {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, s := range m.sessions {
		if s.State == StatePlaying || s.State == StateStarting || s.State == StateLoaded {
			return s
		}
	}
	return nil
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
		result = append(result, s)
	}
	return result
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

func (m *Manager) Shutdown() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id := range m.sessions {
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

	decoder := xml.NewDecoder(strings.NewReader(xmlStr))
	var md Metadata
	var inItem bool

	for {
		tok, err := decoder.Token()
		if err != nil {
			break
		}
		switch t := tok.(type) {
		case xml.StartElement:
			if t.Name.Local == "item" {
				inItem = true
			}
			if inItem {
				switch t.Name.Local {
				case "title":
					decoder.DecodeElement(&md.Title, &t)
				case "creator":
					var v string
					decoder.DecodeElement(&v, &t)
					if md.Artist == "" {
						md.Artist = v
					}
				case "artist":
					decoder.DecodeElement(&md.Artist, &t)
				case "album":
					decoder.DecodeElement(&md.Album, &t)
				case "albumArtURI":
					decoder.DecodeElement(&md.AlbumArtURI, &t)
				}
			}
		case xml.EndElement:
			if t.Name.Local == "item" {
				inItem = false
			}
		}
	}

	return &md
}

var ErrNotFound = errors.New("session not found")

func safeURL(raw string) string {
	if i := strings.Index(raw, "://"); i > 0 {
		if j := strings.Index(raw[i+3:], "@"); j > 0 {
			return raw[:i+3] + "***@" + raw[i+3+j+1:]
		}
	}
	return raw
}
