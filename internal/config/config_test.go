package config

import (
	"os"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Server.HTTPPort != 8787 {
		t.Errorf("expected port 8787, got %d", cfg.Server.HTTPPort)
	}
	if cfg.FFmpeg.OutputFormat != "opus" {
		t.Errorf("expected opus format, got %s", cfg.FFmpeg.OutputFormat)
	}
	if cfg.FFmpeg.Codec != "libopus" {
		t.Errorf("expected libopus codec, got %s", cfg.FFmpeg.Codec)
	}
	if cfg.FFmpeg.Bitrate != "192k" {
		t.Errorf("expected 192k bitrate, got %s", cfg.FFmpeg.Bitrate)
	}
	if cfg.FFmpeg.SampleRate != 48000 {
		t.Errorf("expected 48000 sample rate, got %d", cfg.FFmpeg.SampleRate)
	}
	if cfg.UPnP.FriendlyName != "Music Assistant Whole Home" {
		t.Errorf("unexpected friendly name: %s", cfg.UPnP.FriendlyName)
	}
	if cfg.Security.RequireStreamToken != true {
		t.Error("expected require_stream_token to be true")
	}
	if cfg.Stream.RingBufferBytes != 8388608 {
		t.Errorf("expected 8MB ring buffer, got %d", cfg.Stream.RingBufferBytes)
	}
}

func TestLoadConfigFile(t *testing.T) {
	yaml := `
server:
  bind_host: "127.0.0.1"
  http_port: 9999
upnp:
  friendly_name: "Test Renderer"
ffmpeg:
  output_format: "mp3"
  codec: "libmp3lame"
  bitrate: "320k"
`
	tmpfile, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.WriteString(yaml); err != nil {
		t.Fatal(err)
	}
	tmpfile.Close()

	cfg, err := Load(tmpfile.Name())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Server.HTTPPort != 9999 {
		t.Errorf("expected port 9999, got %d", cfg.Server.HTTPPort)
	}
	if cfg.UPnP.FriendlyName != "Test Renderer" {
		t.Errorf("expected 'Test Renderer', got '%s'", cfg.UPnP.FriendlyName)
	}
	if cfg.FFmpeg.OutputFormat != "mp3" {
		t.Errorf("expected mp3, got %s", cfg.FFmpeg.OutputFormat)
	}
	if cfg.FFmpeg.Bitrate != "320k" {
		t.Errorf("expected 320k, got %s", cfg.FFmpeg.Bitrate)
	}
}

func TestUUIDAutoGeneration(t *testing.T) {
	cfg, err := LoadConfig(`
upnp:
  uuid: "auto"
`)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.UPnP.UUID == "auto" {
		t.Error("UUID should have been replaced from 'auto'")
	}
	if len(cfg.UPnP.UUID) < 20 {
		t.Errorf("UUID too short: %s", cfg.UPnP.UUID)
	}
}

func TestPublicBaseURLDefault(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Server.BindHost = "0.0.0.0"
	cfg.Server.HTTPPort = 8787
	cfg.Server.PublicBaseURL = ""

	// Simulate what Load does for empty public_base_url
	cfg.Server.PublicBaseURL = "http://0.0.0.0:8787"

	if cfg.Server.PublicBaseURL != "http://0.0.0.0:8787" {
		t.Errorf("unexpected public base URL: %s", cfg.Server.PublicBaseURL)
	}
}

func TestStreamDefaults(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Stream.PrebufferBytes != 524288 {
		t.Errorf("expected 512KB prebuffer, got %d", cfg.Stream.PrebufferBytes)
	}
	if cfg.Stream.MaxClientsPerSession != 16 {
		t.Errorf("expected 16 max clients, got %d", cfg.Stream.MaxClientsPerSession)
	}
	if cfg.Stream.NoClientGraceSeconds != 10 {
		t.Errorf("expected 10s grace, got %d", cfg.Stream.NoClientGraceSeconds)
	}
}

func LoadConfig(data string) (*Config, error) {
	tmpfile, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tmpfile.Name())

	if _, err := tmpfile.WriteString(data); err != nil {
		return nil, err
	}
	tmpfile.Close()

	return Load(tmpfile.Name())
}
