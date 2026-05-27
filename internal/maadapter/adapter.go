package maadapter

import (
	"time"

	"github.com/leko/ma-dlna/internal/config"
)

// PlayerStatus holds enriched player state from Music Assistant.
type PlayerStatus struct {
	State            string
	QueueID          string
	Elapsed          time.Duration
	ElapsedUpdatedAt time.Time
	HasElapsed       bool
}

// PlayerClient controls the selected Music Assistant player or group.
type PlayerClient interface {
	Target() string
	PlayMedia(req PlayRequest) error
	Resume() error
	Stop() error
	Pause() error
	Seek(position time.Duration) error
	SetVolume(volume int) error
	GetState() (string, error)
	GetStatus() (PlayerStatus, error)
	PlaybackPosition() (time.Duration, bool, error)
}

// PlayRequest carries the source URL and metadata to send to Music Assistant.
type PlayRequest struct {
	StreamURL   string
	SourceURL   string
	ContentType string
	Title       string
	Artist      string
	Album       string
	AlbumArtURI string
	Duration    string
}

// New returns a Music Assistant WebSocket client.
func New(cfg *config.Config) PlayerClient {
	return newDirectAdapter(cfg)
}
