package config

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server     ServerConfig     `yaml:"server"`
	UPnP       UPnPConfig       `yaml:"upnp"`
	HA         HAConfig         `yaml:"ha"`
	MAAdapter  MAAdapterConfig  `yaml:"ma_adapter"`
	FFmpeg     FFmpegConfig     `yaml:"ffmpeg"`
	Stream     StreamConfig     `yaml:"stream"`
	Security   SecurityConfig   `yaml:"security"`
	Logging    LoggingConfig    `yaml:"logging"`
}

type ServerConfig struct {
	BindHost      string `yaml:"bind_host"`
	HTTPPort      int    `yaml:"http_port"`
	PublicBaseURL string `yaml:"public_base_url"`
}

type UPnPConfig struct {
	FriendlyName           string `yaml:"friendly_name"`
	Manufacturer           string `yaml:"manufacturer"`
	ModelName              string `yaml:"model_name"`
	UUID                   string `yaml:"uuid"`
	AdvertiseIntervalSecs  int    `yaml:"advertise_interval_seconds"`
	AutoBaseURL            bool   `yaml:"auto_base_url"`
}

type HAConfig struct {
	URL             string `yaml:"url"`
	Token           string `yaml:"token"`
	TargetEntityID  string `yaml:"target_entity_id"`
}

type MAAdapterConfig struct {
	Mode                string `yaml:"mode"`
	PlayService         string `yaml:"play_service"`
	FallbackPlayService string `yaml:"fallback_play_service"`
	StopService         string `yaml:"stop_service"`
	PauseService        string `yaml:"pause_service"`
	VolumeService       string `yaml:"volume_service"`
}

type FFmpegConfig struct {
	Binary          string   `yaml:"binary"`
	OutputFormat    string   `yaml:"output_format"`
	Codec           string   `yaml:"codec"`
	Bitrate         string   `yaml:"bitrate"`
	SampleRate      int      `yaml:"sample_rate"`
	Channels        int      `yaml:"channels"`
	Reconnect       bool     `yaml:"reconnect"`
	ExtraInputArgs  []string `yaml:"extra_input_args"`
	ExtraOutputArgs []string `yaml:"extra_output_args"`
}

type StreamConfig struct {
	PrebufferBytes        int `yaml:"prebuffer_bytes"`
	RingBufferBytes       int `yaml:"ring_buffer_bytes"`
	MaxClientsPerSession  int `yaml:"max_clients_per_session"`
	NoClientGraceSeconds  int `yaml:"no_client_grace_seconds"`
	StartupTimeoutSeconds int `yaml:"startup_timeout_seconds"`
}

type SecurityConfig struct {
	RequireStreamToken  bool     `yaml:"require_stream_token"`
	AllowedSourceCIDRs  []string `yaml:"allowed_source_cidrs"`
	BlockedSourceCIDRs  []string `yaml:"blocked_source_cidrs"`
	AllowPublicSources  bool     `yaml:"allow_public_sources"`
}

type LoggingConfig struct {
	Level string `yaml:"level"`
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	expanded := os.ExpandEnv(string(data))

	cfg := DefaultConfig()
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	if cfg.Server.PublicBaseURL == "" {
		cfg.Server.PublicBaseURL = fmt.Sprintf("http://%s:%d", cfg.Server.BindHost, cfg.Server.HTTPPort)
	}

	if cfg.UPnP.UUID == "" || cfg.UPnP.UUID == "auto" {
		cfg.UPnP.UUID = generateUUID()
	}

	return &cfg, nil
}

func DefaultConfig() Config {
	return Config{
		Server: ServerConfig{
			BindHost: "0.0.0.0",
			HTTPPort: 8787,
		},
		UPnP: UPnPConfig{
			FriendlyName:          "Music Assistant Whole Home",
			Manufacturer:          "dlna-ma-bridge",
			ModelName:             "DLNA to Music Assistant Bridge",
			UUID:                  "auto",
			AdvertiseIntervalSecs: 1800,
			AutoBaseURL:           true,
		},
		HA: HAConfig{
			URL:            "http://homeassistant.local:8123",
			TargetEntityID: "media_player.whole_home",
		},
		MAAdapter: MAAdapterConfig{
			Mode:                "ha_service",
			PlayService:         "music_assistant.play_media",
			FallbackPlayService: "media_player.play_media",
			StopService:         "media_player.media_stop",
			PauseService:        "media_player.media_pause",
			VolumeService:       "media_player.volume_set",
		},
		FFmpeg: FFmpegConfig{
			Binary:       "ffmpeg",
			OutputFormat: "opus",
			Codec:        "libopus",
			Bitrate:      "192k",
			SampleRate:   48000,
			Channels:     2,
			Reconnect:    true,
		},
		Stream: StreamConfig{
			PrebufferBytes:        524288,
			RingBufferBytes:       8388608,
			MaxClientsPerSession:  16,
			NoClientGraceSeconds:  10,
			StartupTimeoutSeconds: 15,
		},
		Security: SecurityConfig{
			RequireStreamToken: true,
			AllowedSourceCIDRs: []string{
				"192.168.0.0/16",
				"10.0.0.0/8",
				"172.16.0.0/12",
			},
			BlockedSourceCIDRs: []string{
				"127.0.0.0/8",
				"169.254.0.0/16",
			},
			AllowPublicSources: true,
		},
		Logging: LoggingConfig{
			Level: "info",
		},
	}
}

func generateUUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("uuid:%s-%s-%s-%s-%s",
		hex.EncodeToString(b[0:4]),
		hex.EncodeToString(b[4:6]),
		hex.EncodeToString(b[6:8]),
		hex.EncodeToString(b[8:10]),
		hex.EncodeToString(b[10:16]),
	)
}

// expandEnv replaces ${VAR} with the value of environment variable VAR.
func (c *Config) expandEnv() {
	c.HA.Token = os.ExpandEnv(c.HA.Token)
}

var _ = strings.TrimSpace
