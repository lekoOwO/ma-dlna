package upnp

import (
	"errors"
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
	mu                sync.Mutex
	status            maadapter.PlayerStatus
	playReqs          []maadapter.PlayRequest
	resume            int
	stop              int
	pause             int
	volume            []int
	seek              []time.Duration
	playBlock         <-chan struct{}
	pauseBlock        <-chan struct{}
	resumeBlock       <-chan struct{}
	seekBlock         <-chan struct{}
	seekStarted       chan time.Duration
	playSetsPlaying   bool
	playSetsElapsed   bool
	playElapsed       time.Duration
	pauseSetsPaused   bool
	pauseSetsElapsed  bool
	pauseElapsed      time.Duration
	resumeSetsElapsed bool
	resumeElapsed     time.Duration
	playErr           error
	resumeErr         error
}

func newFakePlayerClient() *fakePlayerClient {
	return &fakePlayerClient{
		status:          maadapter.PlayerStatus{State: "idle", QueueID: "queue123", HasElapsed: true},
		playSetsPlaying: true,
		pauseSetsPaused: true,
	}
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
	if f.playErr != nil {
		return f.playErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.playSetsPlaying {
		f.status.State = "playing"
		if f.playSetsElapsed {
			f.status.Elapsed = f.playElapsed
		} else {
			f.status.Elapsed = 0
		}
		f.status.HasElapsed = true
		f.status.ElapsedUpdatedAt = time.Now()
		f.status.CurrentURI = req.SourceURL
		f.status.CurrentURIs = []string{req.SourceURL}
	}
	return nil
}

func (f *fakePlayerClient) Resume() error {
	f.mu.Lock()
	f.resume++
	block := f.resumeBlock
	f.mu.Unlock()
	if block != nil {
		<-block
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.resumeErr != nil {
		return f.resumeErr
	}
	f.status.State = "playing"
	if f.resumeSetsElapsed {
		f.status.Elapsed = f.resumeElapsed
		f.status.HasElapsed = true
	}
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
	f.pause++
	block := f.pauseBlock
	f.mu.Unlock()
	if block != nil {
		<-block
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pauseSetsPaused {
		f.status.State = "paused"
	}
	if f.pauseSetsElapsed {
		f.status.Elapsed = f.pauseElapsed
		f.status.HasElapsed = true
		f.status.ElapsedUpdatedAt = time.Now()
	}
	return nil
}

func (f *fakePlayerClient) Seek(position time.Duration) error {
	f.mu.Lock()
	block := f.seekBlock
	started := f.seekStarted
	f.mu.Unlock()
	if started != nil {
		started <- position
	}
	if block != nil {
		<-block
	}
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

func (f *fakePlayerClient) setStatusForURI(state string, elapsed time.Duration, uri string) {
	f.setStatusForURIs(state, elapsed, uri, []string{uri})
}

func (f *fakePlayerClient) setStatusForURIs(state string, elapsed time.Duration, currentURI string, uris []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status.State = state
	f.status.Elapsed = elapsed
	f.status.HasElapsed = true
	f.status.ElapsedUpdatedAt = time.Now()
	f.status.CurrentURI = currentURI
	if len(uris) == 0 || (len(uris) == 1 && uris[0] == "") {
		f.status.CurrentURIs = nil
	} else {
		f.status.CurrentURIs = uris
	}
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
	return f.waitForPlayRequests(t, 1)
}

func (f *fakePlayerClient) waitForPlayRequests(t *testing.T, count int) maadapter.PlayRequest {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		playReqs := len(f.playReqs)
		var req maadapter.PlayRequest
		if playReqs > 0 {
			req = f.playReqs[playReqs-1]
		}
		f.mu.Unlock()
		if playReqs >= count {
			return req
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d PlayMedia calls", count)
	return maadapter.PlayRequest{}
}

func (f *fakePlayerClient) waitForPause(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		pauseCalls := f.pause
		f.mu.Unlock()
		if pauseCalls > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for Pause")
}

func (f *fakePlayerClient) waitForResume(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		resumeCalls := f.resume
		f.mu.Unlock()
		if resumeCalls > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for Resume")
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
	client := http.Client{Timeout: 15 * time.Second}
	resp, err := client.Post(baseURL+path, "text/xml; charset=utf-8", strings.NewReader(soapEnvelope(service, action, inner)))
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

func postSOAPStatus(t *testing.T, baseURL, service, action, inner string) (int, string) {
	t.Helper()
	path := "/" + strings.ToLower(service) + "/control"
	if service == "ConnectionManager" {
		path = "/connection/control"
	}
	client := http.Client{Timeout: 15 * time.Second}
	resp, err := client.Post(baseURL+path, "text/xml; charset=utf-8", strings.NewReader(soapEnvelope(service, action, inner)))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

type soapResult struct {
	body string
	err  error
}

func postSOAPAsync(t *testing.T, baseURL, service, action, inner string) <-chan soapResult {
	t.Helper()
	ch := make(chan soapResult, 1)
	go func() {
		path := "/" + strings.ToLower(service) + "/control"
		if service == "ConnectionManager" {
			path = "/connection/control"
		}
		client := http.Client{Timeout: 15 * time.Second}
		resp, err := client.Post(baseURL+path, "text/xml; charset=utf-8", strings.NewReader(soapEnvelope(service, action, inner)))
		if err != nil {
			ch <- soapResult{err: err}
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusOK {
			ch <- soapResult{err: fmt.Errorf("%s expected 200, got %d: %s", action, resp.StatusCode, string(body))}
			return
		}
		ch <- soapResult{body: string(body)}
	}()
	return ch
}

func awaitSOAP(t *testing.T, ch <-chan soapResult, timeout time.Duration) string {
	t.Helper()
	select {
	case res := <-ch:
		if res.err != nil {
			t.Fatal(res.err)
		}
		return res.body
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for SOAP response after %s", timeout)
		return ""
	}
}

func assertNoSOAPResponse(t *testing.T, ch <-chan soapResult, duration time.Duration) {
	t.Helper()
	select {
	case res := <-ch:
		if res.err != nil {
			t.Fatal(res.err)
		}
		t.Fatalf("SOAP response returned before backend confirmation: %s", res.body)
	case <-time.After(duration):
	}
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

func TestPlayWaitsForMusicAssistantPlayingBeforeResponding(t *testing.T) {
	player := newFakePlayerClient()
	player.playSetsPlaying = false
	ts, _, _ := startMAOnlyTestServer(t, player)
	defer ts.Close()

	sourceURL := "http://192.168.1.10/song.mp3"
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+sourceURL+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	playResult := postSOAPAsync(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)
	assertNoSOAPResponse(t, playResult, 100*time.Millisecond)

	stateXML := postSOAP(t, ts.URL, "AVTransport", "GetTransportInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(stateXML, "CurrentTransportState"); got != "TRANSITIONING" {
		t.Fatalf("expected TRANSITIONING before MA confirms playing, got %s", got)
	}

	player.setStatusForURI("playing", 0, "")
	assertNoSOAPResponse(t, playResult, 100*time.Millisecond)

	player.setStatusForURIs("playing", 0, "builtin://track/canonical", []string{"builtin://track/canonical", sourceURL})
	awaitSOAP(t, playResult, 2*time.Second)

	stateXML = postSOAP(t, ts.URL, "AVTransport", "GetTransportInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(stateXML, "CurrentTransportState"); got != "PLAYING" {
		t.Fatalf("expected PLAYING after MA confirms playing, got %s", got)
	}
}

func TestDuplicatePlayWhileStartingWaitsForMusicAssistantPlaying(t *testing.T) {
	player := newFakePlayerClient()
	player.playSetsPlaying = false
	ts, _, _ := startMAOnlyTestServer(t, player)
	defer ts.Close()

	sourceURL := "http://192.168.1.10/song.mp3"
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+sourceURL+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	firstPlay := postSOAPAsync(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)

	secondPlay := postSOAPAsync(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	assertNoSOAPResponse(t, secondPlay, 100*time.Millisecond)

	player.setStatusForURI("playing", 0, sourceURL)
	awaitSOAP(t, firstPlay, 2*time.Second)
	awaitSOAP(t, secondPlay, 2*time.Second)
}

func TestMAOnlyTransportControlsCallSelectedPlayer(t *testing.T) {
	player := newFakePlayerClient()
	ts, _, _ := startMAOnlyTestServer(t, player)
	defer ts.Close()

	sourceURL := "http://192.168.1.10/song.mp3"
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+sourceURL+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
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

func TestPauseDuringStartingIsNotOverwrittenByLatePlayMedia(t *testing.T) {
	player := newFakePlayerClient()
	block := make(chan struct{})
	var releaseOnce sync.Once
	releasePlay := func() { releaseOnce.Do(func() { close(block) }) }
	player.playBlock = block
	ts, _, _ := startMAOnlyTestServer(t, player)
	t.Cleanup(ts.Close)
	t.Cleanup(releasePlay)

	sourceURL := "http://192.168.1.10/song.mp3"
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+sourceURL+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	playResult := postSOAPAsync(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)

	postSOAP(t, ts.URL, "AVTransport", "Pause", "<InstanceID>0</InstanceID>")
	stateXML := postSOAP(t, ts.URL, "AVTransport", "GetTransportInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(stateXML, "CurrentTransportState"); got != "PAUSED_PLAYBACK" {
		t.Fatalf("expected PAUSED_PLAYBACK after pause during start, got %s", got)
	}

	releasePlay()
	awaitSOAP(t, playResult, 2*time.Second)

	stateXML = postSOAP(t, ts.URL, "AVTransport", "GetTransportInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(stateXML, "CurrentTransportState"); got != "PAUSED_PLAYBACK" {
		t.Fatalf("late PlayMedia completion should not resume paused session, got %s", got)
	}
}

func TestPauseDuringStartingClearsGuardWhenLatePlayMediaFails(t *testing.T) {
	player := newFakePlayerClient()
	player.playErr = errors.New("play failed")
	block := make(chan struct{})
	var releaseOnce sync.Once
	releasePlay := func() { releaseOnce.Do(func() { close(block) }) }
	player.playBlock = block
	ts, _, _ := startMAOnlyTestServer(t, player)
	t.Cleanup(ts.Close)
	t.Cleanup(releasePlay)

	sourceURL := "http://192.168.1.10/song.mp3"
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+sourceURL+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	playResult := postSOAPAsync(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)

	postSOAP(t, ts.URL, "AVTransport", "Pause", "<InstanceID>0</InstanceID>")
	releasePlay()
	awaitSOAP(t, playResult, 2*time.Second)

	player.setStatusForURI("playing", 0, sourceURL)
	stateXML := postSOAP(t, ts.URL, "AVTransport", "GetTransportInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(stateXML, "CurrentTransportState"); got != "PLAYING" {
		t.Fatalf("late PlayMedia error should clear pause guard, got %s", got)
	}
}

func TestPauseInFlightDoesNotOverwriteResume(t *testing.T) {
	player := newFakePlayerClient()
	pauseBlock := make(chan struct{})
	var releaseOnce sync.Once
	releasePause := func() { releaseOnce.Do(func() { close(pauseBlock) }) }
	player.pauseBlock = pauseBlock
	ts, _, _ := startMAOnlyTestServer(t, player)
	t.Cleanup(ts.Close)
	t.Cleanup(releasePause)

	sourceURL := "http://192.168.1.10/song.mp3"
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+sourceURL+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)

	pauseResult := postSOAPAsync(t, ts.URL, "AVTransport", "Pause", "<InstanceID>0</InstanceID>")
	player.waitForPause(t)

	playResult := postSOAPAsync(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	awaitSOAP(t, playResult, 2*time.Second)

	releasePause()
	awaitSOAP(t, pauseResult, 2*time.Second)
	player.setStatusForURI("playing", 0, sourceURL)

	stateXML := postSOAP(t, ts.URL, "AVTransport", "GetTransportInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(stateXML, "CurrentTransportState"); got != "PLAYING" {
		t.Fatalf("stale pause completion should not overwrite resumed session, got %s", got)
	}
}

func TestPauseFailsWhenMusicAssistantKeepsPlaying(t *testing.T) {
	player := newFakePlayerClient()
	player.pauseSetsPaused = false
	ts, _, _ := startMAOnlyTestServer(t, player)
	defer ts.Close()

	sourceURL := "http://192.168.1.10/song.mp3"
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+sourceURL+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)

	status, body := postSOAPStatus(t, ts.URL, "AVTransport", "Pause", "<InstanceID>0</InstanceID>")
	if status != http.StatusInternalServerError {
		t.Fatalf("expected Pause to fail when MA remains playing, got status=%d body=%s", status, body)
	}

	stateXML := postSOAP(t, ts.URL, "AVTransport", "GetTransportInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(stateXML, "CurrentTransportState"); got != "PLAYING" {
		t.Fatalf("failed Pause should leave transport PLAYING, got %s", got)
	}
}

func TestFreshPlayRebasesSmallInitialMusicAssistantElapsed(t *testing.T) {
	player := newFakePlayerClient()
	player.playSetsElapsed = true
	player.playElapsed = 2 * time.Second
	ts, _, _ := startMAOnlyTestServer(t, player)
	defer ts.Close()

	sourceURL := "http://192.168.1.10/song.mp3"
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+sourceURL+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")

	player.mu.Lock()
	seekCalls := len(player.seek)
	var lastSeek time.Duration
	if seekCalls > 0 {
		lastSeek = player.seek[seekCalls-1]
	}
	player.mu.Unlock()

	if seekCalls == 0 {
		t.Fatal("expected fresh Play to seek MA back to 0 when initial MA elapsed is already ahead")
	}
	if lastSeek != 0 {
		t.Fatalf("expected fresh Play rebase seek to 0, got %v", lastSeek)
	}

	posXML := postSOAP(t, ts.URL, "AVTransport", "GetPositionInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(posXML, "RelTime"); got != "00:00:00" {
		t.Fatalf("expected fresh Play RelTime to start at 00:00:00 after rebase, got %s", got)
	}
}

func TestFreshPlayDoesNotRebaseTinyInitialMusicAssistantElapsed(t *testing.T) {
	player := newFakePlayerClient()
	player.playSetsElapsed = true
	player.playElapsed = 100 * time.Millisecond
	ts, _, _ := startMAOnlyTestServer(t, player)
	defer ts.Close()

	sourceURL := "http://192.168.1.10/song.mp3"
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+sourceURL+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")

	player.mu.Lock()
	seekCalls := len(player.seek)
	player.mu.Unlock()

	if seekCalls != 0 {
		t.Fatalf("fresh Play should not rebase tiny initial MA elapsed, seek calls=%d", seekCalls)
	}
}

func TestFreshPlayDoesNotRebaseLargeInitialElapsed(t *testing.T) {
	player := newFakePlayerClient()
	player.playSetsElapsed = true
	player.playElapsed = 5 * time.Second
	ts, _, _ := startMAOnlyTestServer(t, player)
	defer ts.Close()

	sourceURL := "http://192.168.1.10/song.mp3"
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+sourceURL+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")

	player.mu.Lock()
	seekCalls := len(player.seek)
	player.mu.Unlock()

	if seekCalls != 0 {
		t.Fatalf("fresh Play should not rebase large initial MA elapsed > max, seek calls=%d", seekCalls)
	}
}

func TestResumeDoesNotTriggerFreshRebaseSeek(t *testing.T) {
	player := newFakePlayerClient()
	player.resumeSetsElapsed = true
	player.resumeElapsed = 1 * time.Second
	ts, _, _ := startMAOnlyTestServer(t, player)
	defer ts.Close()

	sourceURL := "http://192.168.1.10/song.mp3"
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+sourceURL+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)

	player.setStatus("playing", 42*time.Second)
	postSOAP(t, ts.URL, "AVTransport", "Pause", "<InstanceID>0</InstanceID>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")

	player.mu.Lock()
	seekCalls := player.seek
	player.mu.Unlock()

	for _, s := range seekCalls {
		if s == 0 {
			t.Fatal("resume path should not trigger fresh rebase Seek(0)")
		}
	}
}

func TestStaleBackendElapsedSuppressedAfterFreshRebase(t *testing.T) {
	player := newFakePlayerClient()
	player.playSetsElapsed = true
	player.playElapsed = 2 * time.Second
	ts, _, _ := startMAOnlyTestServer(t, player)
	defer ts.Close()

	sourceURL := "http://192.168.1.10/song.mp3"
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+sourceURL+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")

	player.mu.Lock()
	seekCalls := len(player.seek)
	player.mu.Unlock()
	if seekCalls != 1 {
		t.Fatalf("expected fresh rebase Seek(0), got %d seek calls", seekCalls)
	}

	player.setStatus("playing", 2*time.Second)

	posXML := postSOAP(t, ts.URL, "AVTransport", "GetPositionInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(posXML, "RelTime"); got != "00:00:00" {
		t.Fatalf("stale backend elapsed after fresh rebase should not overwrite zero, got %s", got)
	}
}

func TestFreshRebaseHoldDoesNotLeakToNextSession(t *testing.T) {
	player := newFakePlayerClient()
	player.playSetsElapsed = true
	player.playElapsed = 2 * time.Second
	ts, _, _ := startMAOnlyTestServer(t, player)
	defer ts.Close()

	firstURL := "http://192.168.1.10/first.mp3"
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+firstURL+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")

	secondURL := "http://192.168.1.10/second.mp3"
	player.playElapsed = 5 * time.Second
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+secondURL+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")

	posXML := postSOAP(t, ts.URL, "AVTransport", "GetPositionInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(posXML, "RelTime"); got != "00:00:05" {
		t.Fatalf("fresh rebase hold should not suppress next session position, got %s", got)
	}
}

func TestExplicitSeekDuringFreshRebaseWinsAndTransportPlays(t *testing.T) {
	player := newFakePlayerClient()
	player.playSetsElapsed = true
	player.playElapsed = 2 * time.Second
	seekBlock := make(chan struct{})
	player.seekBlock = seekBlock
	player.seekStarted = make(chan time.Duration, 2)
	ts, _, _ := startMAOnlyTestServer(t, player)
	defer ts.Close()

	sourceURL := "http://192.168.1.10/song.mp3"
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+sourceURL+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	playResult := postSOAPAsync(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")

	select {
	case pos := <-player.seekStarted:
		if pos != 0 {
			t.Fatalf("expected fresh rebase seek to start at 0, got %v", pos)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for fresh rebase seek")
	}

	seekResult := postSOAPAsync(t, ts.URL, "AVTransport", "Seek", "<InstanceID>0</InstanceID><Unit>REL_TIME</Unit><Target>00:00:10</Target>")
	close(seekBlock)
	awaitSOAP(t, playResult, 2*time.Second)
	awaitSOAP(t, seekResult, 2*time.Second)

	player.mu.Lock()
	seekCalls := append([]time.Duration(nil), player.seek...)
	player.mu.Unlock()
	if len(seekCalls) != 2 {
		t.Fatalf("expected fresh rebase and explicit seek, got %v", seekCalls)
	}
	if seekCalls[0] != 0 || seekCalls[1] != 10*time.Second {
		t.Fatalf("explicit seek should run after fresh rebase and win, got %v", seekCalls)
	}

	stateXML := postSOAP(t, ts.URL, "AVTransport", "GetTransportInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(stateXML, "CurrentTransportState"); got != "PLAYING" {
		t.Fatalf("transport should not remain TRANSITIONING after seek/rebase race, got %s", got)
	}
}

func TestMAResumeAfterPauseSeeksToCachedPosition(t *testing.T) {
	player := newFakePlayerClient()
	player.resumeSetsElapsed = true
	ts, _, _ := startMAOnlyTestServer(t, player)
	defer ts.Close()

	sourceURL := "http://192.168.1.10/song.mp3"
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+sourceURL+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)

	player.setStatus("playing", 42*time.Second)

	postSOAP(t, ts.URL, "AVTransport", "Pause", "<InstanceID>0</InstanceID>")

	posXML := postSOAP(t, ts.URL, "AVTransport", "GetPositionInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(posXML, "RelTime"); got != "00:00:42" {
		t.Fatalf("expected paused RelTime 00:00:42, got %s", got)
	}

	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")

	player.mu.Lock()
	seekCalls := len(player.seek)
	var lastSeek time.Duration
	if seekCalls > 0 {
		lastSeek = player.seek[seekCalls-1]
	}
	player.mu.Unlock()

	if seekCalls == 0 {
		t.Fatal("expected Seek to be called after Resume to restore cached pause position, but no Seek was called")
	}
	if lastSeek != 42*time.Second {
		t.Fatalf("expected Seek to 42s after Resume, got %v", lastSeek)
	}

	posXML = postSOAP(t, ts.URL, "AVTransport", "GetPositionInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(posXML, "RelTime"); got != "00:00:42" {
		t.Fatalf("resumed RelTime should stay at restored pause position, got %s", got)
	}
}

func TestExternalMusicAssistantPauseFreezesBackendPosition(t *testing.T) {
	player := newFakePlayerClient()
	ts, _, _ := startMAOnlyTestServer(t, player)
	defer ts.Close()

	sourceURL := "http://192.168.1.10/song.mp3"
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+sourceURL+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)

	player.setStatus("playing", 10*time.Second)
	posXML := postSOAP(t, ts.URL, "AVTransport", "GetPositionInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(posXML, "RelTime"); got != "00:00:10" {
		t.Fatalf("expected initial RelTime 00:00:10, got %s", got)
	}

	player.setStatus("paused", 25*time.Second)
	stateXML := postSOAP(t, ts.URL, "AVTransport", "GetTransportInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(stateXML, "CurrentTransportState"); got != "PAUSED_PLAYBACK" {
		t.Fatalf("expected backend pause to sync PAUSED_PLAYBACK, got %s", got)
	}

	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")

	player.mu.Lock()
	seekCalls := len(player.seek)
	var lastSeek time.Duration
	if seekCalls > 0 {
		lastSeek = player.seek[seekCalls-1]
	}
	player.mu.Unlock()

	if seekCalls == 0 {
		t.Fatal("expected Seek to be called after resuming backend-paused session")
	}
	if lastSeek != 25*time.Second {
		t.Fatalf("expected external pause position to be frozen at 25s, got resume seek %v", lastSeek)
	}
}

func TestPauseConfirmationUpdatesCachedResumePosition(t *testing.T) {
	player := newFakePlayerClient()
	player.pauseSetsElapsed = true
	player.pauseElapsed = 13 * time.Second
	ts, _, _ := startMAOnlyTestServer(t, player)
	defer ts.Close()

	sourceURL := "http://192.168.1.10/song.mp3"
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+sourceURL+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)

	player.setStatus("playing", 10*time.Second)
	postSOAP(t, ts.URL, "AVTransport", "Pause", "<InstanceID>0</InstanceID>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")

	player.mu.Lock()
	seekCalls := len(player.seek)
	var lastSeek time.Duration
	if seekCalls > 0 {
		lastSeek = player.seek[seekCalls-1]
	}
	player.mu.Unlock()

	if seekCalls == 0 {
		t.Fatal("expected resume to seek to confirmed paused position")
	}
	if lastSeek != 13*time.Second {
		t.Fatalf("expected resume seek to confirmed 13s pause position, got %v", lastSeek)
	}
}

func TestPauseIdleZeroConfirmationKeepsCachedResumePosition(t *testing.T) {
	player := newFakePlayerClient()
	player.pauseSetsPaused = false
	player.pauseSetsElapsed = true
	ts, _, _ := startMAOnlyTestServer(t, player)
	defer ts.Close()

	sourceURL := "http://192.168.1.10/song.mp3"
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+sourceURL+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)

	player.setStatus("playing", 18*time.Second)
	postSOAP(t, ts.URL, "AVTransport", "GetPositionInfo", "<InstanceID>0</InstanceID>")

	player.mu.Lock()
	player.status.State = "idle"
	player.status.Elapsed = 0
	player.status.HasElapsed = true
	player.status.ElapsedUpdatedAt = time.Now()
	player.mu.Unlock()
	postSOAP(t, ts.URL, "AVTransport", "Pause", "<InstanceID>0</InstanceID>")

	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")

	player.mu.Lock()
	seekCalls := len(player.seek)
	var lastSeek time.Duration
	if seekCalls > 0 {
		lastSeek = player.seek[seekCalls-1]
	}
	player.mu.Unlock()

	if seekCalls == 0 {
		t.Fatal("expected resume to seek to cached pre-pause position after idle zero confirmation")
	}
	if lastSeek.Round(time.Second) != 18*time.Second {
		t.Fatalf("expected idle zero pause confirmation to preserve 18s resume seek, got %v", lastSeek)
	}
}

func TestStoppedSessionDoesNotSeekAfterLateResume(t *testing.T) {
	player := newFakePlayerClient()
	resumeBlock := make(chan struct{})
	var releaseOnce sync.Once
	releaseResume := func() { releaseOnce.Do(func() { close(resumeBlock) }) }
	player.resumeBlock = resumeBlock
	ts, _, _ := startMAOnlyTestServer(t, player)
	t.Cleanup(ts.Close)
	t.Cleanup(releaseResume)

	sourceURL := "http://192.168.1.10/song.mp3"
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+sourceURL+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)

	player.setStatus("playing", 42*time.Second)
	postSOAP(t, ts.URL, "AVTransport", "Pause", "<InstanceID>0</InstanceID>")

	playResult := postSOAPAsync(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForResume(t)

	postSOAP(t, ts.URL, "AVTransport", "Stop", "<InstanceID>0</InstanceID>")
	releaseResume()
	awaitSOAP(t, playResult, 2*time.Second)

	player.mu.Lock()
	seekCalls := len(player.seek)
	stopCalls := player.stop
	backendState := player.status.State
	player.mu.Unlock()
	if seekCalls != 0 {
		t.Fatalf("late resume should not seek after session stopped, seek calls=%d", seekCalls)
	}
	if stopCalls != 2 || backendState != "idle" {
		t.Fatalf("late resume after stop should be stopped again, stop calls=%d backend state=%s", stopCalls, backendState)
	}

	stateXML := postSOAP(t, ts.URL, "AVTransport", "GetTransportInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(stateXML, "CurrentTransportState"); got != "STOPPED" {
		t.Fatalf("expected stopped session to remain STOPPED, got %s", got)
	}
}

func TestStaleResumeDoesNotSeekDuringSameSessionReplay(t *testing.T) {
	player := newFakePlayerClient()
	resumeBlock := make(chan struct{})
	playBlock := make(chan struct{})
	var releaseResumeOnce sync.Once
	var releasePlayOnce sync.Once
	releaseResume := func() { releaseResumeOnce.Do(func() { close(resumeBlock) }) }
	releasePlay := func() { releasePlayOnce.Do(func() { close(playBlock) }) }
	player.resumeBlock = resumeBlock
	ts, _, _ := startMAOnlyTestServer(t, player)
	t.Cleanup(ts.Close)
	t.Cleanup(releaseResume)
	t.Cleanup(releasePlay)

	sourceURL := "http://192.168.1.10/song.mp3"
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+sourceURL+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)

	player.setStatus("playing", 42*time.Second)
	postSOAP(t, ts.URL, "AVTransport", "Pause", "<InstanceID>0</InstanceID>")

	stalePlay := postSOAPAsync(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForResume(t)

	postSOAP(t, ts.URL, "AVTransport", "Stop", "<InstanceID>0</InstanceID>")
	player.mu.Lock()
	player.playBlock = playBlock
	player.mu.Unlock()
	currentPlay := postSOAPAsync(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlayRequests(t, 2)

	releaseResume()
	awaitSOAP(t, stalePlay, 2*time.Second)

	player.mu.Lock()
	seekCalls := len(player.seek)
	player.mu.Unlock()
	if seekCalls != 0 {
		t.Fatalf("stale resume should not seek during newer same-session play, seek calls=%d", seekCalls)
	}

	releasePlay()
	awaitSOAP(t, currentPlay, 2*time.Second)
	stateXML := postSOAP(t, ts.URL, "AVTransport", "GetTransportInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(stateXML, "CurrentTransportState"); got != "PLAYING" {
		t.Fatalf("expected newer same-session play to remain PLAYING, got %s", got)
	}
}

func TestStaleResumeErrorDoesNotFailSameSessionReplay(t *testing.T) {
	player := newFakePlayerClient()
	resumeBlock := make(chan struct{})
	playBlock := make(chan struct{})
	var releaseResumeOnce sync.Once
	var releasePlayOnce sync.Once
	releaseResume := func() { releaseResumeOnce.Do(func() { close(resumeBlock) }) }
	releasePlay := func() { releasePlayOnce.Do(func() { close(playBlock) }) }
	player.resumeBlock = resumeBlock
	player.resumeErr = errors.New("resume failed")
	ts, _, _ := startMAOnlyTestServer(t, player)
	t.Cleanup(ts.Close)
	t.Cleanup(releaseResume)
	t.Cleanup(releasePlay)

	sourceURL := "http://192.168.1.10/song.mp3"
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+sourceURL+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)

	player.setStatus("playing", 42*time.Second)
	postSOAP(t, ts.URL, "AVTransport", "Pause", "<InstanceID>0</InstanceID>")

	stalePlay := postSOAPAsync(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForResume(t)

	postSOAP(t, ts.URL, "AVTransport", "Stop", "<InstanceID>0</InstanceID>")
	player.mu.Lock()
	player.playBlock = playBlock
	player.mu.Unlock()
	currentPlay := postSOAPAsync(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlayRequests(t, 2)

	releaseResume()
	awaitSOAP(t, stalePlay, 2*time.Second)
	releasePlay()
	awaitSOAP(t, currentPlay, 2*time.Second)

	stateXML := postSOAP(t, ts.URL, "AVTransport", "GetTransportInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(stateXML, "CurrentTransportState"); got != "PLAYING" {
		t.Fatalf("stale resume error should not fail newer same-session play, got %s", got)
	}
}

func TestExplicitSeekDuringResumePreventsStaleResumeSeek(t *testing.T) {
	player := newFakePlayerClient()
	resumeBlock := make(chan struct{})
	seekBlock := make(chan struct{})
	var releaseResumeOnce sync.Once
	var releaseSeekOnce sync.Once
	releaseResume := func() { releaseResumeOnce.Do(func() { close(resumeBlock) }) }
	releaseSeek := func() { releaseSeekOnce.Do(func() { close(seekBlock) }) }
	player.resumeBlock = resumeBlock
	player.seekBlock = seekBlock
	ts, _, _ := startMAOnlyTestServer(t, player)
	t.Cleanup(ts.Close)
	t.Cleanup(releaseResume)
	t.Cleanup(releaseSeek)

	sourceURL := "http://192.168.1.10/song.mp3"
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+sourceURL+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	postSOAP(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)

	player.setStatus("playing", 42*time.Second)
	postSOAP(t, ts.URL, "AVTransport", "Pause", "<InstanceID>0</InstanceID>")

	resumeResult := postSOAPAsync(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForResume(t)

	seekResult := postSOAPAsync(t, ts.URL, "AVTransport", "Seek", "<InstanceID>0</InstanceID><Unit>REL_TIME</Unit><Target>00:01:40</Target>")
	time.Sleep(20 * time.Millisecond)
	releaseResume()
	awaitSOAP(t, resumeResult, 2*time.Second)

	player.mu.Lock()
	seekCallsWhileExplicitSeekBlocked := len(player.seek)
	player.mu.Unlock()
	if seekCallsWhileExplicitSeekBlocked != 0 {
		t.Fatalf("stale resume should not issue auto-seek while explicit seek is in flight, seek calls=%d", seekCallsWhileExplicitSeekBlocked)
	}

	releaseSeek()
	awaitSOAP(t, seekResult, 2*time.Second)
	player.mu.Lock()
	seekCalls := len(player.seek)
	lastSeek := player.seek[seekCalls-1]
	player.mu.Unlock()
	if seekCalls != 1 || lastSeek != 100*time.Second {
		t.Fatalf("expected only explicit seek to 100s, calls=%d last=%v", seekCalls, lastSeek)
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
	player.playSetsPlaying = false
	block := make(chan struct{})
	var releaseOnce sync.Once
	releasePlay := func() { releaseOnce.Do(func() { close(block) }) }
	player.playBlock = block
	ts, _, _ := startMAOnlyTestServer(t, player)
	t.Cleanup(ts.Close)
	t.Cleanup(releasePlay)

	sourceURL := "http://192.168.1.10/new-song.mp3"
	player.setStatusForURI("playing", 10*time.Minute, "http://192.168.1.10/old-song.mp3")
	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>"+sourceURL+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	playResult := postSOAPAsync(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)

	posXML := postSOAP(t, ts.URL, "AVTransport", "GetPositionInfo", "<InstanceID>0</InstanceID>")
	if got := extractXMLField(posXML, "RelTime"); got != "00:00:00" {
		t.Fatalf("fresh starting session should not use stale MA position, got %s", got)
	}

	releasePlay()
	assertNoSOAPResponse(t, playResult, 100*time.Millisecond)
	player.setStatusForURI("playing", 0, sourceURL)
	awaitSOAP(t, playResult, 2*time.Second)
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
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releasePlay) }) }
	player.playBlock = releasePlay
	ts, _, _ := startMAOnlyTestServer(t, player)
	t.Cleanup(ts.Close)
	t.Cleanup(release)

	postSOAP(t, ts.URL, "AVTransport", "SetAVTransportURI", "<InstanceID>0</InstanceID><CurrentURI>http://192.168.1.10/song.mp3</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	playResult := postSOAPAsync(t, ts.URL, "AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	player.waitForPlay(t)

	postSOAP(t, ts.URL, "AVTransport", "Seek", "<InstanceID>0</InstanceID><Unit>REL_TIME</Unit><Target>00:00:00</Target>")

	player.mu.Lock()
	seekCalls := len(player.seek)
	player.mu.Unlock()
	if seekCalls != 0 {
		t.Fatalf("startup Seek(0) should not call Music Assistant seek, got %d calls", seekCalls)
	}
	release()
	awaitSOAP(t, playResult, 2*time.Second)
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
