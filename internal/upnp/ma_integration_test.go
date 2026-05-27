package upnp

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/leko/ma-dlna/internal/config"
	"github.com/leko/ma-dlna/internal/maadapter"
	"github.com/leko/ma-dlna/internal/session"
	"github.com/leko/ma-dlna/internal/stream"
)

type fakePlayerClient struct {
	mu        sync.Mutex
	status    maadapter.PlayerStatus
	playReqs  []maadapter.PlayRequest
	resume    int
	stop      int
	pause     int
	volume    []int
	seek      []time.Duration
	playBlock <-chan struct{}
}

func newFakePlayerClient() *fakePlayerClient {
	return &fakePlayerClient{status: maadapter.PlayerStatus{State: "idle", QueueID: "queue123", HasElapsed: true}}
}

func (f *fakePlayerClient) Target() string { return "player123" }

func (f *fakePlayerClient) PlayMedia(req maadapter.PlayRequest) error {
	f.mu.Lock()
	f.playReqs = append(f.playReqs, req)
	block := f.playBlock
	f.mu.Unlock()
	if block != nil {
		<-block
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status.State = "playing"
	f.status.HasElapsed = true
	f.status.ElapsedUpdatedAt = time.Now()
	return nil
}

func (f *fakePlayerClient) Resume() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resume++
	f.status.State = "playing"
	f.status.ElapsedUpdatedAt = time.Now()
	return nil
}

func (f *fakePlayerClient) Stop() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stop++
	f.status.State = "idle"
	return nil
}

func (f *fakePlayerClient) Pause() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pause++
	f.status.State = "paused"
	return nil
}

func (f *fakePlayerClient) Seek(position time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seek = append(f.seek, position)
	f.status.Elapsed = position
	f.status.HasElapsed = true
	f.status.ElapsedUpdatedAt = time.Now()
	return nil
}

func (f *fakePlayerClient) SetVolume(volume int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.volume = append(f.volume, volume)
	return nil
}

func (f *fakePlayerClient) GetState() (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status.State, nil
}

func (f *fakePlayerClient) GetStatus() (maadapter.PlayerStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.status, nil
}

func (f *fakePlayerClient) PlaybackPosition() (time.Duration, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.status.HasElapsed {
		return 0, false, nil
	}
	pos := f.status.Elapsed
	if f.status.State == "playing" && !f.status.ElapsedUpdatedAt.IsZero() {
		pos += time.Since(f.status.ElapsedUpdatedAt)
	}
	return pos, true, nil
}

func (f *fakePlayerClient) setStatus(state string, elapsed time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status.State = state
	f.status.Elapsed = elapsed
	f.status.HasElapsed = true
	f.status.ElapsedUpdatedAt = time.Now()
}

func (f *fakePlayerClient) lastPlayRequest() (maadapter.PlayRequest, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.playReqs) == 0 {
		return maadapter.PlayRequest{}, false
	}
	return f.playReqs[len(f.playReqs)-1], true
}

func (f *fakePlayerClient) waitForPlay(t *testing.T) maadapter.PlayRequest {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if req, ok := f.lastPlayRequest(); ok {
			return req
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for PlayMedia")
	return maadapter.PlayRequest{}
}

func startMAOnlyTestServer(t *testing.T, player *fakePlayerClient) (*httptest.Server, *session.Manager, *stream.Streamer) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Security.AllowLoopbackSources = true
	cfg.Security.AllowedSourceCIDRs = append(cfg.Security.AllowedSourceCIDRs, "127.0.0.0/8")
	cfg.UPnP.AutoBaseURL = false
	cfg.Server.PublicBaseURL = "http://bridge:8787"
	cfg.MusicAssistant.URL = "http://ma.local:8095"
	cfg.MusicAssistant.Token = "test-token"
	cfg.MusicAssistant.TargetPlayerID = "player123"

	strm := stream.NewStreamer(&cfg)
	sm := session.NewManager(&cfg, strm)
	h := NewHandler(&cfg, sm, player)
	strm.SetTokenValidator(sm.ValidateToken)

	mux := http.NewServeMux()
	h.RegisterUPnPEndpoints(mux)
	return httptest.NewServer(mux), sm, strm
}

func postSOAP(t *testing.T, baseURL, service, action, inner string) string {
	t.Helper()
	path := "/" + strings.ToLower(service) + "/control"
	if service == "ConnectionManager" {
		path = "/connection/control"
	}
	resp, err := http.Post(baseURL+path, "text/xml; charset=utf-8", strings.NewReader(soapEnvelope(service, action, inner)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s expected 200, got %d: %s", action, resp.StatusCode, string(body))
	}
	return string(body)
}

func TestMAOnlyPlayUsesSourceURLAndDoesNotStartBridgeStream(t *testing.T) {
	player := newFakePlayerClient()
	ts, sm, strm := startMAOnlyTestServer(t, player)
	defer ts.Close()

	sourceURL := "http://192.168.1.10/song.mp3?token=source"
	metadata := `<DIDL-Lite><item><title>Direct Song</title><res protocolInfo="http-get:*:audio/mpeg:*" duration="00:03:00">` + sourceURL + `</res></item></DIDL-Lite>`
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+sourceURL+"</CurrentURI><CurrentURIMetaData>"+escapeXMLText(metadata)+"</CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")

	req := player.waitForPlay(t)
	if req.SourceURL != sourceURL {
		t.Fatalf("PlayMedia should receive source URL, got %s", req.SourceURL)
	}
	if req.StreamURL == "" || !strings.Contains(req.StreamURL, "/live/") {
		t.Fatalf("session stream URL should still be generated for compatibility, got %s", req.StreamURL)
	}
	if req.ContentType != "audio/mpeg" {
		t.Fatalf("expected metadata content type audio/mpeg, got %s", req.ContentType)
	}
	active := sm.CurrentSession()
	if active == nil {
		t.Fatal("expected active session")
	}
	if strm.IsRunning(active.ID) {
		t.Fatal("MA-only playback must not start ffmpeg bridge stream")
	}
	stateXML := postSOAP(t, ts.URL, "AVTransport", "GetTransportInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(stateXML, "CurrentTransportState"); got != "PLAYING" {
		t.Fatalf("expected PLAYING, got %s", got)
	}
}

func TestMAOnlyTransportControlsCallSelectedPlayer(t *testing.T) {
	player := newFakePlayerClient()
	ts, _, _ := startMAOnlyTestServer(t, player)
	defer ts.Close()

	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>http://192.168.1.10/song.mp3</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)

	postSOAP(t, ts.URL, "AVTransport", "Pause", "<InstanceID>0</InstanceID>")
	stateXML := postSOAP(t, ts.URL, "AVTransport", "GetTransportInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(stateXML, "CurrentTransportState"); got != "PAUSED_PLAYBACK" {
		t.Fatalf("expected PAUSED_PLAYBACK, got %s", got)
	}

	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	postSOAP(t, ts.URL, "AVTransport", "Seek", "<InstanceID>0</InstanceID><Unit>REL_TIME</Unit><Target>00:00:42</Target>")
	postSOAP(t, ts.URL, "AVTransport", "Stop", "<InstanceID>0</InstanceID>")

	player.mu.Lock()
	defer player.mu.Unlock()
	if player.pause != 1 || player.resume != 1 || player.stop != 1 {
		t.Fatalf("unexpected control counts pause=%d resume=%d stop=%d", player.pause, player.resume, player.stop)
	}
	if len(player.seek) != 1 || player.seek[0] != 42*time.Second {
		t.Fatalf("unexpected seek calls: %v", player.seek)
	}
}

func TestGetPositionInfoUsesMusicAssistantPosition(t *testing.T) {
	player := newFakePlayerClient()
	ts, _, _ := startMAOnlyTestServer(t, player)
	defer ts.Close()

	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>http://192.168.1.10/song.mp3</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)
	player.setStatus("playing", 12*time.Second)

	posXML := postSOAP(t, ts.URL, "AVTransport", "GetPositionInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(posXML, "RelTime"); got != "00:00:12" {
		t.Fatalf("expected RelTime 00:00:12, got %s", got)
	}
}

func TestGetPositionInfoDoesNotRewindWhenPausedMusicAssistantReportsIdleZero(t *testing.T) {
	player := newFakePlayerClient()
	ts, _, _ := startMAOnlyTestServer(t, player)
	defer ts.Close()

	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>http://192.168.1.10/song.mp3</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)
	player.setStatus("playing", 15*time.Second)

	posXML := postSOAP(t, ts.URL, "AVTransport", "GetPositionInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(posXML, "RelTime"); got != "00:00:15" {
		t.Fatalf("expected initial RelTime 00:00:15, got %s", got)
	}

	postSOAP(t, ts.URL, "AVTransport", "Pause", "<InstanceID>0</InstanceID>")
	player.setStatus("idle", 0)

	posXML = postSOAP(t, ts.URL, "AVTransport", "GetPositionInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(posXML, "RelTime"); got != "00:00:15" {
		t.Fatalf("paused RelTime should keep last known position, got %s", got)
	}

	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.setStatus("playing", 0)

	posXML = postSOAP(t, ts.URL, "AVTransport", "GetPositionInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(posXML, "RelTime"); got != "00:00:15" {
		t.Fatalf("resumed RelTime should keep cached position, got %s", got)
	}
}

func TestGetPositionInfoAcceptsExternalBackwardSeekFromMusicAssistant(t *testing.T) {
	player := newFakePlayerClient()
	ts, _, _ := startMAOnlyTestServer(t, player)
	defer ts.Close()

	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>http://192.168.1.10/song.mp3</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)
	player.setStatus("playing", 2*time.Minute)

	posXML := postSOAP(t, ts.URL, "AVTransport", "GetPositionInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(posXML, "RelTime"); got != "00:02:00" {
		t.Fatalf("expected initial RelTime 00:02:00, got %s", got)
	}

	player.setStatus("playing", 30*time.Second)

	posXML = postSOAP(t, ts.URL, "AVTransport", "GetPositionInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(posXML, "RelTime"); got != "00:00:30" {
		t.Fatalf("expected external backward seek to be reflected, got %s", got)
	}
}

func TestGetPositionInfoDuringFreshStartIgnoresStaleMusicAssistantPosition(t *testing.T) {
	player := newFakePlayerClient()
	block := make(chan struct{})
	player.playBlock = block
	defer close(block)
	ts, _, _ := startMAOnlyTestServer(t, player)
	defer ts.Close()

	player.setStatus("playing", 10*time.Minute)
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>http://192.168.1.10/new-song.mp3</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)

	posXML := postSOAP(t, ts.URL, "AVTransport", "GetPositionInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(posXML, "RelTime"); got != "00:00:00" {
		t.Fatalf("fresh starting session should not use stale MA position, got %s", got)
	}
}

func TestGetTransportInfoSyncsExternalPauseFromMusicAssistant(t *testing.T) {
	player := newFakePlayerClient()
	ts, _, _ := startMAOnlyTestServer(t, player)
	defer ts.Close()

	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>http://192.168.1.10/song.mp3</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)
	player.setStatus("paused", 7*time.Second)

	stateXML := postSOAP(t, ts.URL, "AVTransport", "GetTransportInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(stateXML, "CurrentTransportState"); got != "PAUSED_PLAYBACK" {
		t.Fatalf("expected external MA pause to sync to PAUSED_PLAYBACK, got %s", got)
	}
	posXML := postSOAP(t, ts.URL, "AVTransport", "GetPositionInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(posXML, "RelTime"); got != "00:00:07" {
		t.Fatalf("expected paused MA position 00:00:07, got %s", got)
	}
}

func TestGetTransportInfoKeepsPausedWhenMusicAssistantReportsIdle(t *testing.T) {
	player := newFakePlayerClient()
	ts, _, _ := startMAOnlyTestServer(t, player)
	defer ts.Close()

	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>http://192.168.1.10/song.mp3</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)
	player.setStatus("playing", 15*time.Second)

	postSOAP(t, ts.URL, "AVTransport", "Pause", "<InstanceID>0</InstanceID>")
	player.setStatus("idle", 0)

	stateXML := postSOAP(t, ts.URL, "AVTransport", "GetTransportInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(stateXML, "CurrentTransportState"); got != "PAUSED_PLAYBACK" {
		t.Fatalf("expected paused session to remain PAUSED_PLAYBACK when MA reports idle, got %s", got)
	}
}

func escapeXMLText(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	return s
}

func TestGetTransportInfoSyncsExternalStopFromMusicAssistant(t *testing.T) {
	player := newFakePlayerClient()
	ts, _, _ := startMAOnlyTestServer(t, player)
	defer ts.Close()

	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>http://192.168.1.10/song.mp3</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)
	player.setStatus("idle", 0)

	stateXML := postSOAP(t, ts.URL, "AVTransport", "GetTransportInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(stateXML, "CurrentTransportState"); got != "STOPPED" {
		t.Fatalf("expected external MA idle to sync to STOPPED, got %s", got)
	}
}

func TestStartupSeekZeroIsNoOpWhilePlayMediaIsPending(t *testing.T) {
	player := newFakePlayerClient()
	releasePlay := make(chan struct{})
	player.playBlock = releasePlay
	ts, _, _ := startMAOnlyTestServer(t, player)
	defer ts.Close()

	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>http://192.168.1.10/song.mp3</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)

	postSOAP(t, ts.URL, "AVTransport", "Seek", "<InstanceID>0</InstanceID><Unit>REL_TIME</Unit><Target>00:00:00</Target>")

	player.mu.Lock()
	seekCalls := len(player.seek)
	player.mu.Unlock()
	if seekCalls != 0 {
		t.Fatalf("startup Seek(0) should not call Music Assistant seek, got %d calls", seekCalls)
	}
	close(releasePlay)
}

func extractXMLField(xmlText, field string) string {
	start := strings.Index(xmlText, "<"+field+">")
	if start < 0 {
		return ""
	}
	start += len(field) + 2
	end := strings.Index(xmlText[start:], "</"+field+">")
	if end < 0 {
		return ""
	}
	return xmlText[start : start+end]
}

func soapEnvelope(service, action, inner string) string {
	return fmt.Sprintf(`<?xml version="1.0"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
  <s:Body>
    <u:%s xmlns:u="urn:schemas-upnp-org:service:%s:1">%s</u:%s>
  </s:Body>
</s:Envelope>`, action, service, inner, action)
}
