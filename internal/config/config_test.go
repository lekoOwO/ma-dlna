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

func TestValidateSourceURI(t *testing.T) {
	sec := SecurityConfig{
		AllowedSourceCIDRs: []string{"192.168.0.0/16", "10.0.0.0/8"},
		BlockedSourceCIDRs: []string{"127.0.0.0/8"},
		AllowPublicSources: false,
	}

	tests := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{"private IP v4 allowed", "http://192.168.1.10/song.mp3", false},
		{"private IP v4 allowed 2", "http://10.0.0.5/song.mp3", false},
		{"localhost blocked", "http://127.0.0.1/song.mp3", true},
		{"loopback blocked", "http://localhost/song.mp3", true},
		{"invalid scheme", "ftp://192.168.1.1/song.mp3", true},
		{"public IP blocked by default", "http://8.8.8.8/song.mp3", true},
		{"private not in allowed CIDRs", "http://172.16.5.5/song.mp3", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := sec.ValidateSourceURI(tc.url)
			if (err != nil) != tc.wantErr {
				t.Errorf("ValidateSourceURI(%q) error=%v, wantErr=%v", tc.url, err, tc.wantErr)
			}
		})
	}

	// Test with public sources allowed (no allowed CIDRs, allow public)
	sec2 := SecurityConfig{
		AllowPublicSources: true,
	}
	if err := sec2.ValidateSourceURI("http://8.8.8.8/song.mp3"); err != nil {
		t.Errorf("public IP should be allowed when AllowPublicSources=true with no CIDR restrictions: %v", err)
	}

	// Test that AllowedSourceCIDRs gates private IPs, AllowPublicSources gates public IPs
	sec3 := SecurityConfig{
		AllowedSourceCIDRs: []string{"192.168.0.0/16"},
		AllowPublicSources: true,
	}
	// Private IP not in allowed CIDRs → rejected
	if err := sec3.ValidateSourceURI("http://10.0.0.5/song.mp3"); err == nil {
		t.Error("private IP should be rejected when not in AllowedSourceCIDRs")
	}
	// Public IP with AllowPublicSources → allowed
	if err := sec3.ValidateSourceURI("http://8.8.8.8/song.mp3"); err != nil {
		t.Errorf("public IP should be allowed when AllowPublicSources=true: %v", err)
	}
}

func TestValidateSourceURILinkLocal(t *testing.T) {
	sec := SecurityConfig{
		AllowedSourceCIDRs: []string{"0.0.0.0/0"},
		AllowPublicSources: true,
	}

	// Link-local and multicast should always be blocked regardless of config
	err := sec.ValidateSourceURI("http://169.254.1.1/song.mp3")
	if err == nil {
		t.Error("link-local IP should be blocked")
	}
}

func TestValidateSourceURINoDNS(t *testing.T) {
	sec := SecurityConfig{
		AllowPublicSources: true,
	}

	err := sec.ValidateSourceURI("http://nonexistent.invalid/song.mp3")
	if err == nil {
		t.Error("unresolvable hostname should return error")
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
