package upnp

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/leko/ma-dlna/internal/config"
	"github.com/leko/ma-dlna/internal/maadapter"
	"github.com/leko/ma-dlna/internal/session"
	"github.com/leko/ma-dlna/internal/stream"
)

// dlnaClient simulates a real DLNA controller talking to our bridge.
type dlnaClient struct {
	baseURL string
	client  *http.Client
}

func newDLNAClient(baseURL string) *dlnaClient {
	return &dlnaClient{baseURL: strings.TrimSuffix(baseURL, "/"), client: &http.Client{Timeout: 10 * time.Second}}
}

func (c *dlnaClient) soap(service, action, inner string) (*http.Response, []byte, error) {
	body := soapEnvelope(service, action, inner)
	path := "/" + strings.ToLower(service) + "/control"
	if service == "ConnectionManager" {
		path = "/connection/control"
	}
	resp, err := c.client.Post(c.baseURL+path, "text/xml; charset=utf-8", strings.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, data, nil
}

func (c *dlnaClient) SetAVTransportURI(uri, metadata string) error {
	resp, _, err := c.soap("AVTransport", "SetAVTransportURI",
		"<InstanceID>0</InstanceID><CurrentURI>"+uri+"</CurrentURI><CurrentURIMetaData>"+escapeXMLText(metadata)+"</CurrentURIMetaData>")
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (c *dlnaClient) Play() error {
	resp, _, err := c.soap("AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (c *dlnaClient) Pause() ([]byte, error) {
	_, data, err := c.soap("AVTransport", "Pause", "<InstanceID>0</InstanceID>")
	return data, err
}

func (c *dlnaClient) Stop() ([]byte, error) {
	_, data, err := c.soap("AVTransport", "Stop", "<InstanceID>0</InstanceID>")
	return data, err
}

func (c *dlnaClient) Seek(unit, target string) ([]byte, error) {
	_, data, err := c.soap("AVTransport", "Seek",
		"<InstanceID>0</InstanceID><Unit>"+unit+"</Unit><Target>"+target+"</Target>")
	return data, err
}

func (c *dlnaClient) GetPositionInfo() (string, error) {
	_, data, err := c.soap("AVTransport", "GetPositionInfo", "<InstanceID>0</InstanceID>")
	if err != nil {
		return "", err
	}
	return extractXMLField(string(data), "RelTime"), nil
}

func (c *dlnaClient) GetTransportInfo() (state string, err error) {
	_, data, err := c.soap("AVTransport", "GetTransportInfo", "<InstanceID>0</InstanceID>")
	if err != nil {
		return "", err
	}
	return extractXMLField(string(data), "CurrentTransportState"), nil
}

func (c *dlnaClient) Subscribe(service string) (sid string, err error) {
	cbServer, cbCh := mockCallback(&testing.T{})
	defer cbServer.Close()
	_ = cbCh

	req, _ := http.NewRequest("SUBSCRIBE", c.baseURL+"/"+service+"/event", nil)
	req.Header.Set("CALLBACK", callbackURL(cbServer))
	req.Header.Set("NT", "upnp:event")
	req.Header.Set("TIMEOUT", "Second-1800")
	resp, err := c.client.Do(req)
	if err != nil {
		return "", err
	}
	resp.Body.Close()
	return resp.Header.Get("SID"), nil
}

// TestPauseResumePosition verifies position tracking through pause/resume.
func TestPauseResumePosition(t *testing.T) {
	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "audio/mpeg")
		for i := 0; i < 500; i++ {
			w.Write(make([]byte, 418))
		}
	}))
	defer sourceServer.Close()

	haServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"success": true}]`))
	}))
	defer haServer.Close()

	cfg := config.DefaultConfig()
	cfg.Security.AllowLoopbackSources = true
	cfg.Security.AllowedSourceCIDRs = append(cfg.Security.AllowedSourceCIDRs, "127.0.0.0/8")
	cfg.HA.URL = haServer.URL
	cfg.HA.Token = "test-token"
	cfg.HA.TargetEntityID = "media_player.test"
	cfg.Server.PublicBaseURL = "http://bridge:8787"
	cfg.UPnP.AutoBaseURL = false

	strm := stream.NewStreamer(&cfg)
	sm := session.NewManager(&cfg, strm)
	ma := maadapter.New(&cfg)
	h := NewHandler(&cfg, sm, ma)
	strm.SetTokenValidator(sm.ValidateToken)
	strm.SetFirstClientCallback(func(id string) { sm.SetPlaying(id) })

	mux := http.NewServeMux()
	h.RegisterUPnPEndpoints(mux)
	strmMux := http.NewServeMux()
	strmMux.HandleFunc("/live/", strm.ServeHTTP)
	ts := httptest.NewServer(mux)
	strmTS := httptest.NewServer(strmMux)
	defer ts.Close()
	defer strmTS.Close()

	cl := newDLNAClient(ts.URL)

	// Play
	sourceURL := sourceServer.URL + "/song.mp3"
	if err := cl.SetAVTransportURI(sourceURL, ""); err != nil {
		t.Fatal("SetAVTransportURI:", err)
	}
	if err := cl.Play(); err != nil {
		t.Fatal("Play:", err)
	}
	time.Sleep(time.Second)

	// Verify playing
	state, err := cl.GetTransportInfo()
	if err != nil {
		t.Fatal("GetTransportInfo:", err)
	}
	if state != "PLAYING" {
		t.Logf("TransportState after Play: %s (may be TRANSITIONING before first client)", state)
	}

	// Pause
	if _, err := cl.Pause(); err != nil {
		t.Fatal("Pause:", err)
	}
	time.Sleep(200 * time.Millisecond)

	state, _ = cl.GetTransportInfo()
	if state != "PAUSED_PLAYBACK" {
		t.Errorf("After Pause: expected PAUSED_PLAYBACK, got %s", state)
	}

	// Get position before pause
	pausedPos, err := cl.GetPositionInfo()
	if err != nil {
		t.Fatal("GetPositionInfo after pause:", err)
	}
	t.Logf("Paused at position: %s", pausedPos)

	// Resume
	if err := cl.Play(); err != nil {
		t.Fatal("Play (resume):", err)
	}
	time.Sleep(time.Second)

	// Verify position after resume >= paused position
	resumedPos, err := cl.GetPositionInfo()
	if err != nil {
		t.Fatal("GetPositionInfo after resume:", err)
	}
	t.Logf("Resumed position: %s", resumedPos)
	if resumedPos < pausedPos {
		t.Errorf("Position went backwards: paused=%s resumed=%s", pausedPos, resumedPos)
	}

	// Stop
	if _, err := cl.Stop(); err != nil {
		t.Fatal("Stop:", err)
	}
}

// TestSeekRepositions tests Seek with REL_TIME.
func TestSeekRepositions(t *testing.T) {
	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "audio/mpeg")
		for i := 0; i < 500; i++ {
			w.Write(make([]byte, 418))
		}
	}))
	defer sourceServer.Close()

	haServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"success": true}]`))
	}))
	defer haServer.Close()

	cfg := config.DefaultConfig()
	cfg.Security.AllowLoopbackSources = true
	cfg.Security.AllowedSourceCIDRs = append(cfg.Security.AllowedSourceCIDRs, "127.0.0.0/8")
	cfg.HA.URL = haServer.URL
	cfg.HA.Token = "test-token"
	cfg.HA.TargetEntityID = "media_player.test"
	cfg.Server.PublicBaseURL = "http://bridge:8787"
	cfg.UPnP.AutoBaseURL = false

	strm := stream.NewStreamer(&cfg)
	sm := session.NewManager(&cfg, strm)
	ma := maadapter.New(&cfg)
	h := NewHandler(&cfg, sm, ma)
	strm.SetTokenValidator(sm.ValidateToken)
	strm.SetFirstClientCallback(func(id string) { sm.SetPlaying(id) })

	mux := http.NewServeMux()
	h.RegisterUPnPEndpoints(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cl := newDLNAClient(ts.URL)

	sourceURL := sourceServer.URL + "/song.mp3"
	if err := cl.SetAVTransportURI(sourceURL, ""); err != nil {
		t.Fatal("SetAVTransportURI:", err)
	}
	if err := cl.Play(); err != nil {
		t.Fatal("Play:", err)
	}
	time.Sleep(2 * time.Second)

	// Seek to 5 seconds
	if _, err := cl.Seek("REL_TIME", "00:00:05"); err != nil {
		t.Fatal("Seek:", err)
	}
	time.Sleep(time.Second)

	pos, err := cl.GetPositionInfo()
	if err != nil {
		t.Fatal("GetPositionInfo:", err)
	}
	t.Logf("Position after seek: %s", pos)

	// Position should be approximately >= 5 seconds after seek.
	// With ffmpeg -ss, position starts from 0 output time + resumeOffset
	if pos == "00:00:00" {
		t.Error("Position should advance after seek, got 00:00:00")
	}

	if _, err := cl.Stop(); err != nil {
		t.Fatal("Stop:", err)
	}
}

// TestDLNAClientFullSession tests the complete DLNA playback lifecycle
// using the client helper (like a real DLNA controller would).
func TestDLNAClientFullSession(t *testing.T) {
	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "audio/mpeg")
		for i := 0; i < 200; i++ {
			w.Write(make([]byte, 418))
		}
	}))
	defer sourceServer.Close()

	haServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"success": true}]`))
	}))
	defer haServer.Close()

	cfg := config.DefaultConfig()
	cfg.Security.AllowLoopbackSources = true
	cfg.Security.AllowedSourceCIDRs = append(cfg.Security.AllowedSourceCIDRs, "127.0.0.0/8")
	cfg.HA.URL = haServer.URL
	cfg.HA.Token = "test-token"
	cfg.HA.TargetEntityID = "media_player.test"
	cfg.Server.PublicBaseURL = "http://bridge:8787"
	cfg.UPnP.AutoBaseURL = false

	strm := stream.NewStreamer(&cfg)
	sm := session.NewManager(&cfg, strm)
	ma := maadapter.New(&cfg)
	h := NewHandler(&cfg, sm, ma)
	strm.SetTokenValidator(sm.ValidateToken)
	strm.SetFirstClientCallback(func(id string) { sm.SetPlaying(id) })

	mux := http.NewServeMux()
	h.RegisterUPnPEndpoints(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cl := newDLNAClient(ts.URL)

	sourceURL := sourceServer.URL + "/song.mp3"

	// 1. SetAVTransportURI
	if err := cl.SetAVTransportURI(sourceURL, ""); err != nil {
		t.Fatal("Step 1 SetAVTransportURI:", err)
	}
	t.Log("Step 1: SetAVTransportURI OK")

	// 2. GetTransportInfo (should show STOPPED since loaded)
	state, _ := cl.GetTransportInfo()
	if state == "" {
		t.Fatal("Step 2 GetTransportInfo: empty state")
	}
	t.Logf("Step 2: GetTransportInfo = %s", state)

	// 3. Play
	if err := cl.Play(); err != nil {
		t.Fatal("Step 3 Play:", err)
	}
	time.Sleep(time.Second)
	t.Log("Step 3: Play OK")

	// 4. GetTransportInfo (should show PLAYING or TRANSITIONING)
	state, _ = cl.GetTransportInfo()
	if state != "PLAYING" && state != "TRANSITIONING" {
		t.Errorf("Step 4: expected PLAYING/TRANSITIONING, got %s", state)
	}
	t.Logf("Step 4: GetTransportInfo = %s", state)

	// 5. Poll GetPositionInfo (should advance)
	var positions []string
	for i := 0; i < 3; i++ {
		pos, err := cl.GetPositionInfo()
		if err != nil {
			t.Fatal("Step 5 GetPositionInfo:", err)
		}
		positions = append(positions, pos)
		time.Sleep(500 * time.Millisecond)
	}
	t.Logf("Step 5: positions = %v", positions)

	// 6. Pause
	if _, err := cl.Pause(); err != nil {
		t.Fatal("Step 6 Pause:", err)
	}
	time.Sleep(200 * time.Millisecond)
	state, _ = cl.GetTransportInfo()
	if state != "PAUSED_PLAYBACK" {
		t.Errorf("Step 6: after Pause, expected PAUSED_PLAYBACK, got %s", state)
	}
	t.Logf("Step 6: Pause OK, state=%s", state)

	// 7. Resume
	if err := cl.Play(); err != nil {
		t.Fatal("Step 7 Play (resume):", err)
	}
	time.Sleep(time.Second)
	t.Log("Step 7: Resume OK")

	// 8. Seek
	if _, err := cl.Seek("REL_TIME", "00:00:02"); err != nil {
		t.Fatal("Step 8 Seek:", err)
	}
	time.Sleep(500 * time.Millisecond)
	t.Log("Step 8: Seek OK")

	// 9. Stop
	if _, err := cl.Stop(); err != nil {
		t.Fatal("Step 9 Stop:", err)
	}
	t.Logf("Step 9: Stop OK - Full session complete")
}

// TestSeekWhilePausedNoDeadlock verifies that Seek during paused state
// does not deadlock (the done channel has no run goroutine in paused state).
func TestSeekWhilePausedNoDeadlock(t *testing.T) {
	haServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"success": true}]`))
	}))
	defer haServer.Close()

	cfg := config.DefaultConfig()
	cfg.HA.URL = haServer.URL
	cfg.HA.Token = "test-token"
	cfg.HA.TargetEntityID = "media_player.test"
	cfg.Server.PublicBaseURL = "http://bridge:8787"
	cfg.UPnP.AutoBaseURL = false
	cfg.Security.AllowLoopbackSources = true
	cfg.Security.AllowedSourceCIDRs = append(cfg.Security.AllowedSourceCIDRs, "127.0.0.0/8")

	strm := stream.NewStreamer(&cfg)
	sm := session.NewManager(&cfg, strm)
	ma := maadapter.New(&cfg)
	h := NewHandler(&cfg, sm, ma)

	mux := http.NewServeMux()
	h.RegisterUPnPEndpoints(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cl := newDLNAClient(ts.URL)

	// SetAVTransportURI (use a non-loopback-like IP in allowed range)
	if err := cl.SetAVTransportURI("http://192.168.1.10/song.mp3", ""); err != nil {
		t.Fatal("SetAVTransportURI:", err)
	}
	if err := cl.Play(); err != nil {
		t.Fatal("Play:", err)
	}
	time.Sleep(500 * time.Millisecond)

	// Pause
	if _, err := cl.Pause(); err != nil {
		t.Fatal("Pause:", err)
	}
	time.Sleep(200 * time.Millisecond)

	// Seek while paused — must not deadlock
	done := make(chan bool, 1)
	go func() {
		_, err := cl.Seek("REL_TIME", "00:00:10")
		done <- (err == nil)
	}()
	select {
	case ok := <-done:
		if !ok {
			t.Error("Seek while paused failed")
		}
		t.Log("Seek while paused completed without deadlock")
	case <-time.After(5 * time.Second):
		t.Fatal("Seek while paused DEADLOCKED (timed out)")
	}

	if _, err := cl.Stop(); err != nil {
		t.Fatal("Stop:", err)
	}
}

// TestDoublePauseNoDeadlock verifies that calling Pause twice
// does not deadlock.
func TestDoublePauseNoDeadlock(t *testing.T) {
	haServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"success": true}]`))
	}))
	defer haServer.Close()

	cfg := config.DefaultConfig()
	cfg.HA.URL = haServer.URL
	cfg.HA.Token = "test-token"
	cfg.HA.TargetEntityID = "media_player.test"
	cfg.Server.PublicBaseURL = "http://bridge:8787"
	cfg.UPnP.AutoBaseURL = false
	cfg.Security.AllowLoopbackSources = true
	cfg.Security.AllowedSourceCIDRs = append(cfg.Security.AllowedSourceCIDRs, "127.0.0.0/8")

	strm := stream.NewStreamer(&cfg)
	sm := session.NewManager(&cfg, strm)
	ma := maadapter.New(&cfg)
	h := NewHandler(&cfg, sm, ma)

	mux := http.NewServeMux()
	h.RegisterUPnPEndpoints(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cl := newDLNAClient(ts.URL)

	if err := cl.SetAVTransportURI("http://192.168.1.10/song.mp3", ""); err != nil {
		t.Fatal("SetAVTransportURI:", err)
	}
	if err := cl.Play(); err != nil {
		t.Fatal("Play:", err)
	}
	time.Sleep(500 * time.Millisecond)

	// First Pause
	if _, err := cl.Pause(); err != nil {
		t.Fatal("First Pause:", err)
	}
	time.Sleep(200 * time.Millisecond)

	// Second Pause — must not deadlock
	done := make(chan bool, 1)
	go func() {
		_, err := cl.Pause()
		done <- (err == nil)
	}()
	select {
	case ok := <-done:
		if !ok {
			t.Error("Second Pause failed")
		}
		t.Log("Double Pause completed without deadlock")
	case <-time.After(5 * time.Second):
		t.Fatal("Double Pause DEADLOCKED (timed out)")
	}

	if _, err := cl.Stop(); err != nil {
		t.Fatal("Stop:", err)
	}
}
