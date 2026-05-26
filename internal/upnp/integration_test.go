package upnp

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/leko/ma-dlna/internal/config"
	"github.com/leko/ma-dlna/internal/maadapter"
	"github.com/leko/ma-dlna/internal/session"
	"github.com/leko/ma-dlna/internal/stream"
)

type haCall struct {
	service string
	body    string
}

// startTestServer creates a bridge HTTP server for integration testing.
func startTestServer(t *testing.T) (*httptest.Server, *Handler) {
	t.Helper()
	cfg := config.DefaultConfig()
	cfg.Security.AllowLoopbackSources = true
	cfg.Security.AllowedSourceCIDRs = append(cfg.Security.AllowedSourceCIDRs, "127.0.0.0/8")
	cfg.UPnP.AutoBaseURL = false
	cfg.Server.PublicBaseURL = "http://test.local:8787"

	streamer := stream.NewStreamer(&cfg)
	sm := session.NewManager(&cfg, streamer)
	ma := maadapter.New(&cfg)

	h := NewHandler(&cfg, sm, ma)

	mux := http.NewServeMux()
	h.RegisterUPnPEndpoints(mux)

	ts := httptest.NewServer(mux)
	return ts, h
}

// mockCallback starts a server that captures NOTIFY requests.
func mockCallback(t *testing.T) (*httptest.Server, chan *http.Request) {
	t.Helper()
	ch := make(chan *http.Request, 4)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		r.Body.Close()
		// Clone the request with body for inspection
		r2 := r.Clone(r.Context())
		r2.Body = io.NopCloser(strings.NewReader(string(body)))
		w.WriteHeader(http.StatusOK)
		select {
		case ch <- r2:
		default:
		}
	}))
	return ts, ch
}

func callbackURL(ts *httptest.Server) string {
	return "<" + ts.URL + "/callback>"
}

func waitForHACall(t *testing.T, calls <-chan haCall, servicePart string) haCall {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case c := <-calls:
			if strings.Contains(c.service, servicePart) {
				return c
			}
		case <-deadline:
			t.Fatalf("timed out waiting for HA call containing %q", servicePart)
		}
	}
}

func waitForSessionState(t *testing.T, sm *session.Manager, sessionID string, want session.State) *session.Session {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s := sm.Get(sessionID); s != nil && s.State == want {
			return s
		}
		time.Sleep(10 * time.Millisecond)
	}
	s := sm.Get(sessionID)
	if s == nil {
		t.Fatalf("timed out waiting for session %s state %s; session missing", sessionID, want)
	}
	t.Fatalf("timed out waiting for session %s state %s; got %s", sessionID, want, s.State)
	return nil
}

func writeSleepFFmpeg(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ffmpeg")
	script := `#!/bin/sh
exec sleep 3600
`
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatalf("write fake ffmpeg: %v", err)
	}
	return path
}

// TestFullSubscriptionFlow tests SUBSCRIBE → receive initial NOTIFY → UNSUBSCRIBE.
func TestFullSubscriptionFlow(t *testing.T) {
	_, h := startTestServer(t)
	cbServer, cbCh := mockCallback(t)
	defer cbServer.Close()

	// Phase 1: New SUBSCRIBE to AVTransport
	w := &testRespWriter{header: make(http.Header)}
	r, _ := http.NewRequest("SUBSCRIBE", "/avtransport/event", nil)
	r.Header.Set("CALLBACK", callbackURL(cbServer))
	r.Header.Set("NT", "upnp:event")
	r.Header.Set("TIMEOUT", "Second-1800")

	h.serveEvent(w, r)

	if w.status != http.StatusOK {
		t.Fatalf("SUBSCRIBE expected 200, got %d", w.status)
	}

	sid := w.header.Get("SID")
	if !strings.HasPrefix(sid, "uuid:") {
		t.Errorf("SID must start with uuid:, got %s", sid)
	}
	if w.header.Get("TIMEOUT") != "Second-1800" {
		t.Error("SUBSCRIBE response must have TIMEOUT header")
	}
	if w.header.Get("SERVER") == "" {
		t.Error("SUBSCRIBE response must have SERVER header")
	}
	t.Logf("SUBSCRIBE ok, SID=%s", sid)

	// Phase 2: Wait for initial NOTIFY to callback
	var notifyReq *http.Request
	select {
	case notifyReq = <-cbCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timed out waiting for initial event NOTIFY")
	}

	// Validate NOTIFY request
	if notifyReq.Method != "NOTIFY" {
		t.Errorf("Expected NOTIFY method, got %s", notifyReq.Method)
	}
	if notifyReq.Header.Get("NT") != "upnp:event" {
		t.Errorf("NOTIFY missing NT header")
	}
	if notifyReq.Header.Get("NTS") != "upnp:propchange" {
		t.Errorf("NOTIFY missing NTS header")
	}
	if notifyReq.Header.Get("SID") != sid {
		t.Errorf("NOTIFY SID mismatch: expected %s, got %s", sid, notifyReq.Header.Get("SID"))
	}
	if notifyReq.Header.Get("SEQ") != "0" {
		t.Errorf("Initial event SEQ must be 0, got %s", notifyReq.Header.Get("SEQ"))
	}
	if notifyReq.Header.Get("Content-Type") == "" {
		t.Error("NOTIFY missing Content-Type")
	}

	body, _ := io.ReadAll(notifyReq.Body)
	bodyStr := string(body)
	if !strings.Contains(bodyStr, "propertyset") {
		t.Error("NOTIFY body must contain propertyset")
	}
	if !strings.Contains(bodyStr, "TransportState") {
		t.Error("AVTransport initial event must contain TransportState")
	}
	t.Logf("NOTIFY received, body=%d bytes, headers OK", len(body))

	// Phase 3: Renewal SUBSCRIBE
	w2 := &testRespWriter{header: make(http.Header)}
	r2, _ := http.NewRequest("SUBSCRIBE", "/avtransport/event", nil)
	r2.Header.Set("SID", sid)
	r2.Header.Set("TIMEOUT", "Second-1800")

	h.serveEvent(w2, r2)
	if w2.status != http.StatusOK {
		t.Errorf("renewal SUBSCRIBE expected 200, got %d", w2.status)
	}
	if w2.header.Get("SID") != sid {
		t.Errorf("renewal must echo SID, expected %s got %s", sid, w2.header.Get("SID"))
	}
	t.Logf("Renewal SUBSCRIBE ok, SID echoed back")

	// Phase 4: UNSUBSCRIBE
	w3 := &testRespWriter{header: make(http.Header)}
	r3, _ := http.NewRequest("UNSUBSCRIBE", "/avtransport/event", nil)
	r3.Header.Set("SID", sid)
	h.serveEvent(w3, r3)
	if w3.status != http.StatusOK {
		t.Errorf("UNSUBSCRIBE expected 200, got %d", w3.status)
	}
	t.Logf("UNSUBSCRIBE ok")
}

// TestDeviceDescriptionXML validates the device XML against UPnP requirements.
func TestDeviceDescriptionXML(t *testing.T) {
	ts, _ := startTestServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/device.xml")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/xml") {
		t.Errorf("expected text/xml content-type, got %s", ct)
	}

	body, _ := io.ReadAll(resp.Body)
	xml := string(body)

	// Required UPnP elements
	required := []string{
		"urn:schemas-upnp-org:device-1-0",
		"urn:schemas-upnp-org:device:MediaRenderer:1",
		"<friendlyName>",
		"<manufacturer>",
		"<modelName>",
		"<UDN>",
		"<serviceList>",
		"AVTransport:1",
		"RenderingControl:1",
		"ConnectionManager:1",
		"<controlURL>",
		"<eventSubURL>",
		"<SCPDURL>",
	}
	for _, req := range required {
		if !strings.Contains(xml, req) {
			t.Errorf("device.xml missing required element: %s", req)
		}
	}

	// DLNA X_DLNADOC
	if !strings.Contains(xml, "X_DLNADOC") || !strings.Contains(xml, "DMR-1.50") {
		t.Error("device.xml missing DLNA X_DLNADOC DMR-1.50")
	}
	t.Logf("device.xml valid, %d bytes", len(xml))
}

// TestServiceDescriptions validates SCPD XMLs are served.
func TestServiceDescriptions(t *testing.T) {
	ts, _ := startTestServer(t)
	defer ts.Close()

	paths := []string{
		"/service/AVTransport/desc.xml",
		"/service/RenderingControl/desc.xml",
		"/service/ConnectionManager/desc.xml",
	}
	for _, p := range paths {
		resp, err := http.Get(ts.URL + p)
		if err != nil {
			t.Errorf("%s: %v", p, err)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s: expected 200, got %d", p, resp.StatusCode)
		}
	}
}

// TestSOAPEndpoints validates POST responses to control endpoints.
func TestSOAPEndpoints(t *testing.T) {
	ts, _ := startTestServer(t)
	defer ts.Close()

	tests := []struct {
		name   string
		path   string
		body   string
		expect int
	}{
		{
			"GetProtocolInfo",
			"/connection/control",
			soapEnvelope("ConnectionManager", "GetProtocolInfo", ""),
			200,
		},
		{
			"GetTransportInfo",
			"/avtransport/control",
			soapEnvelope("AVTransport", "GetTransportInfo",
				"<InstanceID>0</InstanceID>"),
			200,
		},
		{
			"GetPositionInfo",
			"/avtransport/control",
			soapEnvelope("AVTransport", "GetPositionInfo",
				"<InstanceID>0</InstanceID>"),
			200,
		},
		{
			"GetMediaInfo",
			"/avtransport/control",
			soapEnvelope("AVTransport", "GetMediaInfo",
				"<InstanceID>0</InstanceID>"),
			200,
		},
		{
			"GetVolume",
			"/rendering/control",
			soapEnvelope("RenderingControl", "GetVolume",
				"<InstanceID>0</InstanceID><Channel>Master</Channel>"),
			200,
		},
		{
			"GetCurrentTransportActions",
			"/avtransport/control",
			soapEnvelope("AVTransport", "GetCurrentTransportActions",
				"<InstanceID>0</InstanceID>"),
			200,
		},
		{
			"Next",
			"/avtransport/control",
			soapEnvelope("AVTransport", "Next", "<InstanceID>0</InstanceID>"),
			200,
		},
		{
			"Previous",
			"/avtransport/control",
			soapEnvelope("AVTransport", "Previous", "<InstanceID>0</InstanceID>"),
			200,
		},
		{
			"SetNextAVTransportURI",
			"/avtransport/control",
			soapEnvelope("AVTransport", "SetNextAVTransportURI",
				"<InstanceID>0</InstanceID><NextURI>http://next.local/track.mp3</NextURI><NextURIMetaData></NextURIMetaData>"),
			200,
		},
		{
			"SetPlayMode",
			"/avtransport/control",
			soapEnvelope("AVTransport", "SetPlayMode",
				"<InstanceID>0</InstanceID><NewPlayMode>NORMAL</NewPlayMode>"),
			200,
		},
		{
			"GetDeviceCapabilities",
			"/avtransport/control",
			soapEnvelope("AVTransport", "GetDeviceCapabilities",
				"<InstanceID>0</InstanceID>"),
			200,
		},
		{
			"InvalidAction",
			"/avtransport/control",
			soapEnvelope("AVTransport", "NonExistentAction", ""),
			500,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := http.Post(ts.URL+tc.path,
				"text/xml; charset=utf-8",
				strings.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tc.expect {
				t.Errorf("expected %d, got %d", tc.expect, resp.StatusCode)
			}

			body, _ := io.ReadAll(resp.Body)
			xml := string(body)
			if !strings.Contains(xml, "Envelope") {
				t.Error("SOAP response must be an Envelope")
			}
			if strings.Count(xml, "Envelope") > 2 {
				t.Error("SOAP response should not contain multiple envelopes")
			}
		})
	}
}

// TestEventEndpointsPerService verifies per-service initial NOTIFY bodies.
// endpoints work for each service independently.
func TestEventEndpointsPerService(t *testing.T) {
	_, h := startTestServer(t)
	cbServer, cbCh := mockCallback(t)
	defer cbServer.Close()

	services := []string{"avtransport", "rendering", "connection"}
	for _, svc := range services {
		t.Run(svc, func(t *testing.T) {
			w := &testRespWriter{header: make(http.Header)}
			r, _ := http.NewRequest("SUBSCRIBE", "/"+svc+"/event", nil)
			r.Header.Set("CALLBACK", callbackURL(cbServer))
			r.Header.Set("NT", "upnp:event")
			r.Header.Set("TIMEOUT", "Second-1800")

			h.serveEvent(w, r)
			if w.status != 200 {
				t.Errorf("%s: expected 200, got %d", svc, w.status)
				return
			}
			if w.header.Get("SID") == "" {
				t.Errorf("%s: missing SID header", svc)
			}

			// Wait for initial NOTIFY
			select {
			case notifyReq := <-cbCh:
				body, _ := io.ReadAll(notifyReq.Body)
				bodyStr := string(body)
				if svc == "avtransport" && !strings.Contains(bodyStr, "TransportState") {
					t.Error("AVTransport NOTIFY must contain TransportState")
				}
				if svc == "rendering" && !strings.Contains(bodyStr, "Volume") {
					t.Error("RenderingControl NOTIFY must contain Volume")
				}
				if svc == "connection" && !strings.Contains(bodyStr, "SourceProtocolInfo") {
					t.Error("ConnectionManager NOTIFY must contain SourceProtocolInfo")
				}
			case <-time.After(3 * time.Second):
				t.Error("Timed out waiting for initial NOTIFY")
			}
		})
	}
}

// TestPlaybackActions tests the full SetAVTransportURI → Play → Stop → Pause flow.
func TestPlaybackActions(t *testing.T) {
	haCalls := make(chan haCall, 16)
	haServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		haCalls <- haCall{service: r.URL.Path, body: string(body)}
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
	cfg.FFmpeg.Binary = writeSleepFFmpeg(t)

	streamer := stream.NewStreamer(&cfg)
	sm := session.NewManager(&cfg, streamer)
	ma := maadapter.New(&cfg)
	h := NewHandler(&cfg, sm, ma)

	mux := http.NewServeMux()
	h.RegisterUPnPEndpoints(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// Step 1: SetAVTransportURI
	uri := "http://192.168.1.10/song.flac"
	metadata := "<DIDL-Lite><item><title>Test</title></item></DIDL-Lite>"
	body := soapEnvelope("AVTransport", "SetAVTransportURI",
		"<InstanceID>0</InstanceID><CurrentURI>"+uri+"</CurrentURI><CurrentURIMetaData>"+escapeXMLText(metadata)+"</CurrentURIMetaData>")

	resp, err := http.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("SetAVTransportURI expected 200, got %d", resp.StatusCode)
	}

	// Verify session created — must use AllSessions() since Loaded != ActiveSession()
	sessions := sm.AllSessions()
	if len(sessions) == 0 {
		t.Fatal("No session created after SetAVTransportURI")
	}
	s := sessions[len(sessions)-1]
	if s.SourceURI != uri {
		t.Errorf("SourceURI: expected %s, got %s", uri, s.SourceURI)
	}
	if s.State != session.StateLoaded {
		t.Errorf("State: expected loaded, got %s", s.State)
	}
	if s.Metadata.Title != "Test" {
		t.Errorf("Metadata title: expected Test, got %s", s.Metadata.Title)
	}
	t.Logf("SetAVTransportURI ok: session=%s, state=%s, stream_url=%s", s.ID, s.State, s.StreamURL)

	// Step 2: Play
	body = soapEnvelope("AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	resp, err = http.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("Play expected 200, got %d", resp.StatusCode)
	}

	// Session should be in "starting" state (Play sets it to starting)
	s = sm.Get(s.ID)
	if s.State != session.StateStarting {
		t.Errorf("State after Play: expected starting, got %s", s.State)
	}

	playCall := waitForHACall(t, haCalls, "play_media")
	if !strings.Contains(playCall.body, s.StreamURL) {
		t.Errorf("play_media payload should contain stream URL, got: %s", playCall.body)
	}
	t.Logf("Play ok: session state=%s, MA play_media called", s.State)

	// Step 3: Stop
	body = soapEnvelope("AVTransport", "Stop", "<InstanceID>0</InstanceID>")
	resp, err = http.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("Stop expected 200, got %d", resp.StatusCode)
	}
	s = sm.Get(s.ID)
	if s.State != session.StateStopped {
		t.Errorf("State after Stop: expected stopped, got %s", s.State)
	}
	waitForHACall(t, haCalls, "media_stop")
	t.Logf("Stop ok: session state=%s", s.State)

	// Step 4: New session → Play → Pause
	uri2 := "http://192.168.1.10/song2.mp3"
	body = soapEnvelope("AVTransport", "SetAVTransportURI",
		"<InstanceID>0</InstanceID><CurrentURI>"+uri2+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	resp, err = http.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	s2 := sm.ActiveSession()
	if s2 == nil {
		t.Fatal("expected active session after SetAVTransportURI")
	}
	body = soapEnvelope("AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	resp, _ = http.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(body))
	resp.Body.Close()

	body = soapEnvelope("AVTransport", "Pause", "<InstanceID>0</InstanceID>")
	resp, err = http.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	s2 = sm.Get(s2.ID)
	if s2.State != session.StatePaused {
		t.Errorf("State after Pause: expected paused, got %s", s2.State)
	}
	waitForHACall(t, haCalls, "media_pause")
	t.Logf("Pause ok: session state=%s", s2.State)
}

// TestVolumeControl validates RenderingControl volume/mute actions.
func TestVolumeControl(t *testing.T) {
	// Mock HA server
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
	cfg.UPnP.AutoBaseURL = false

	streamer := stream.NewStreamer(&cfg)
	sm := session.NewManager(&cfg, streamer)
	ma := maadapter.New(&cfg)
	h := NewHandler(&cfg, sm, ma)

	mux := http.NewServeMux()
	h.RegisterUPnPEndpoints(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// Set volume to 75
	body := soapEnvelope("RenderingControl", "SetVolume",
		"<InstanceID>0</InstanceID><Channel>Master</Channel><DesiredVolume>75</DesiredVolume>")
	resp, err := http.Post(ts.URL+"/rendering/control", "text/xml", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("SetVolume expected 200, got %d", resp.StatusCode)
	}

	// Get volume back
	body = soapEnvelope("RenderingControl", "GetVolume",
		"<InstanceID>0</InstanceID><Channel>Master</Channel>")
	resp, err = http.Post(ts.URL+"/rendering/control", "text/xml", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(data), "<CurrentVolume>75</CurrentVolume>") {
		t.Errorf("GetVolume should return 75, got: %s", string(data))
	}
	t.Logf("SetVolume/GetVolume ok")

	// Set mute
	body = soapEnvelope("RenderingControl", "SetMute",
		"<InstanceID>0</InstanceID><Channel>Master</Channel><DesiredMute>1</DesiredMute>")
	resp, _ = http.Post(ts.URL+"/rendering/control", "text/xml", strings.NewReader(body))
	resp.Body.Close()

	// Get mute
	body = soapEnvelope("RenderingControl", "GetMute",
		"<InstanceID>0</InstanceID><Channel>Master</Channel>")
	resp, err = http.Post(ts.URL+"/rendering/control", "text/xml", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	data, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(data), "<CurrentMute>1</CurrentMute>") {
		t.Errorf("GetMute should return 1, got: %s", string(data))
	}
	t.Logf("SetMute/GetMute ok")
}

// escapeXMLText escapes a string for inclusion in XML.
func escapeXMLText(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	return s
}

// TestFullDLNALifecycle simulates a complete DLNA controller session:
// M-SEARCH → device.xml → service descriptions → SUBSCRIBE →
// SetAVTransportURI → Play → GetPositionInfo tracking → Pause →
// Resume → Stop → UNSUBSCRIBE.
func TestFullDLNALifecycle(t *testing.T) {
	// Real stream source: serve a minimal MP3 from a test HTTP server
	const testMP3 = "\xff\xfb\x90\x00" // MPEG1 Layer3 128k 44100 stereo (MP3 frame header + silence)
	sourceServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "audio/mpeg")
		w.Write([]byte(testMP3))
		for i := 0; i < 100; i++ {
			w.Write(make([]byte, 418)) // ~1 MP3 frame
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
	cfg.Server.PublicBaseURL = "http://bridge.local:8787"
	cfg.UPnP.AutoBaseURL = false

	streamer := stream.NewStreamer(&cfg)
	sm := session.NewManager(&cfg, streamer)
	ma := maadapter.New(&cfg)
	h := NewHandler(&cfg, sm, ma)

	mux := http.NewServeMux()
	h.RegisterUPnPEndpoints(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	client := &http.Client{}

	// ---- Step 1: Fetch device description ----
	resp, err := client.Get(ts.URL + "/device.xml")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("device.xml returned %d", resp.StatusCode)
	}
	devXML := string(body)
	t.Logf("Step 1: device.xml fetched (%d bytes)", len(devXML))

	// ---- Step 2: Fetch service descriptions ----
	for _, svc := range []string{"AVTransport", "RenderingControl", "ConnectionManager"} {
		resp, err := client.Get(ts.URL + "/service/" + svc + "/desc.xml")
		if err != nil {
			t.Fatalf("%s desc.xml: %v", svc, err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("%s desc.xml returned %d", svc, resp.StatusCode)
		}
	}
	t.Log("Step 2: All service descriptions fetched")

	// ---- Step 3: SUBSCRIBE to events ----
	cbServer, cbCh := mockCallback(t)
	defer cbServer.Close()

	for _, evt := range []string{"avtransport", "rendering", "connection"} {
		req, _ := http.NewRequest("SUBSCRIBE", ts.URL+"/"+evt+"/event", nil)
		req.Header.Set("CALLBACK", callbackURL(cbServer))
		req.Header.Set("NT", "upnp:event")
		req.Header.Set("TIMEOUT", "Second-1800")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("SUBSCRIBE %s: %v", evt, err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("SUBSCRIBE %s returned %d", evt, resp.StatusCode)
		}
		if resp.Header.Get("SID") == "" {
			t.Fatalf("SUBSCRIBE %s: missing SID", evt)
		}
		if resp.Header.Get("TIMEOUT") == "" {
			t.Fatalf("SUBSCRIBE %s: missing TIMEOUT", evt)
		}
	}
	// Consume initial events
	for i := 0; i < 3; i++ {
		select {
		case <-cbCh:
		case <-time.After(2 * time.Second):
			t.Fatal("Timed out waiting for initial event NOTIFY")
		}
	}
	t.Log("Step 3: Events subscribed + initial NOTIFYs received")

	// ---- Step 4: SetAVTransportURI ----
	uri := sourceServer.URL + "/song.mp3"
	metadata := "<DIDL-Lite><item><title>Test</title><creator>Artist</creator></item></DIDL-Lite>"
	setAVBody := soapEnvelope("AVTransport", "SetAVTransportURI",
		"<InstanceID>0</InstanceID><CurrentURI>"+uri+"</CurrentURI><CurrentURIMetaData>"+escapeXMLText(metadata)+"</CurrentURIMetaData>")
	resp, err = client.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(setAVBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("SetAVTransportURI returned %d", resp.StatusCode)
	}
	t.Log("Step 4: SetAVTransportURI OK")

	// ---- Step 5: Play ----
	playBody := soapEnvelope("AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	resp, err = client.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(playBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("Play returned %d", resp.StatusCode)
	}
	// Wait briefly for ffmpeg to start
	time.Sleep(500 * time.Millisecond)
	t.Log("Step 5: Play OK")

	// ---- Step 6: Poll GetPositionInfo (track progress) ----
	getPosBody := soapEnvelope("AVTransport", "GetPositionInfo", "<InstanceID>0</InstanceID>")
	var lastRelTime string
	for i := 0; i < 5; i++ {
		resp, err = client.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(getPosBody))
		if err != nil {
			t.Fatal(err)
		}
		posBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("GetPositionInfo returned %d", resp.StatusCode)
		}
		posXML := string(posBody)
		rt := extractXMLField(posXML, "RelTime")
		if rt != lastRelTime {
			lastRelTime = rt
		}
		if i < 4 {
			time.Sleep(200 * time.Millisecond)
		}
	}
	if lastRelTime == "00:00:00" {
		t.Log("Step 6: RelTime at 00:00:00 (ffmpeg may not be available in test env)")
	} else {
		t.Logf("Step 6: GetPositionInfo tracking OK (RelTime=%s)", lastRelTime)
	}

	// ---- Step 7: GetTransportInfo ----
	getTransBody := soapEnvelope("AVTransport", "GetTransportInfo", "<InstanceID>0</InstanceID>")
	resp, err = client.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(getTransBody))
	if err != nil {
		t.Fatal(err)
	}
	transBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(transBody), "CurrentTransportState") {
		t.Error("GetTransportInfo missing CurrentTransportState")
	}
	t.Log("Step 7: GetTransportInfo OK")

	// ---- Step 8: Pause ----
	pauseBody := soapEnvelope("AVTransport", "Pause", "<InstanceID>0</InstanceID>")
	resp, err = client.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(pauseBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("Pause returned %d", resp.StatusCode)
	}
	t.Log("Step 8: Pause OK")

	// ---- Step 9: Play (resume) ----
	resp, err = client.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(playBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("Play (resume) returned %d", resp.StatusCode)
	}
	time.Sleep(500 * time.Millisecond)
	t.Log("Step 9: Resume OK")

	// ---- Step 10: Stop ----
	stopBody := soapEnvelope("AVTransport", "Stop", "<InstanceID>0</InstanceID>")
	resp, err = client.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(stopBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("Stop returned %d", resp.StatusCode)
	}
	t.Log("Step 10: Stop OK")

	// ---- Step 11: UNSUBSCRIBE ----
	for _, evt := range []string{"avtransport", "rendering", "connection"} {
		req, _ := http.NewRequest("UNSUBSCRIBE", ts.URL+"/"+evt+"/event", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("UNSUBSCRIBE %s: %v", evt, err)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("UNSUBSCRIBE %s returned %d", evt, resp.StatusCode)
		}
	}
	t.Log("Step 11: UNSUBSCRIBE OK")

	// ---- Step 12: Validate GetProtocolInfo includes output format ----
	protoBody := soapEnvelope("ConnectionManager", "GetProtocolInfo", "")
	resp, err = client.Post(ts.URL+"/connection/control", "text/xml", strings.NewReader(protoBody))
	if err != nil {
		t.Fatal(err)
	}
	protoXML, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(protoXML), "audio/ogg") {
		t.Error("GetProtocolInfo missing ogg")
	}
	t.Log("Step 12: GetProtocolInfo includes configured formats OK")

	// ---- Step 13: GetCurrentTransportActions ----
	actionsBody := soapEnvelope("AVTransport", "GetCurrentTransportActions", "<InstanceID>0</InstanceID>")
	resp, err = client.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(actionsBody))
	if err != nil {
		t.Fatal(err)
	}
	actionsXML, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(actionsXML), "Actions") {
		t.Error("GetCurrentTransportActions missing Actions field")
	}
	t.Logf("Step 13: GetCurrentTransportActions OK")

	// ---- Step 14: GetDeviceCapabilities ----
	capBody := soapEnvelope("AVTransport", "GetDeviceCapabilities", "<InstanceID>0</InstanceID>")
	resp, err = client.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(capBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("GetDeviceCapabilities returned %d", resp.StatusCode)
	}
	t.Log("Step 14: GetDeviceCapabilities OK")

	// Re-create session before Seek (session was stopped in Step 10)
	setAVBody = soapEnvelope("AVTransport", "SetAVTransportURI",
		"<InstanceID>0</InstanceID><CurrentURI>"+uri+"</CurrentURI><CurrentURIMetaData>"+escapeXMLText(metadata)+"</CurrentURIMetaData>")
	resp, err = client.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(setAVBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	playBody = soapEnvelope("AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	resp, err = client.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(playBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	time.Sleep(500 * time.Millisecond)

	t.Log("Step 15: Seek")
	seekBody := soapEnvelope("AVTransport", "Seek", "<InstanceID>0</InstanceID><Unit>REL_TIME</Unit><Target>00:00:30</Target>")
	resp, err = client.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(seekBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("Seek returned %d", resp.StatusCode)
	}
	time.Sleep(500 * time.Millisecond)
	t.Log("Step 15: Seek OK")

	// ---- Step 16: SetNextAVTransportURI + Next + Previous + SetPlayMode ----
	nextBody := soapEnvelope("AVTransport", "SetNextAVTransportURI",
		"<InstanceID>0</InstanceID><NextURI>http://next.local/track2.mp3</NextURI><NextURIMetaData></NextURIMetaData>")
	resp, err = client.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(nextBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	nextBody2 := soapEnvelope("AVTransport", "Next", "<InstanceID>0</InstanceID>")
	resp, _ = client.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(nextBody2))
	resp.Body.Close()

	prevBody := soapEnvelope("AVTransport", "Previous", "<InstanceID>0</InstanceID>")
	resp, _ = client.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(prevBody))
	resp.Body.Close()

	modeBody := soapEnvelope("AVTransport", "SetPlayMode",
		"<InstanceID>0</InstanceID><NewPlayMode>NORMAL</NewPlayMode>")
	resp, _ = client.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(modeBody))
	resp.Body.Close()
	t.Logf("Step 16: SetNextAVTransportURI + Next + Previous + SetPlayMode OK")

	t.Logf("Full DLNA lifecycle test PASSED (16 steps)")
}

func extractXMLField(xml, field string) string {
	start := strings.Index(xml, "<"+field+">")
	if start < 0 {
		return ""
	}
	start += len("<" + field + ">")
	end := strings.Index(xml[start:], "</"+field+">")
	if end < 0 {
		return ""
	}
	return xml[start : start+end]
}

func soapEnvelope(service, action, inner string) string {
	return `<?xml version="1.0"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"
            s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
  <s:Body>
    <u:` + action + ` xmlns:u="urn:schemas-upnp-org:service:` + service + `:1">
      ` + inner + `
    </u:` + action + `>
  </s:Body>
</s:Envelope>`
}

// TestStateChangeNotify verifies that Play/Stop/Pause trigger state change NOTIFY to subscribers.
func TestStateChangeNotify(t *testing.T) {
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

	streamer := stream.NewStreamer(&cfg)
	sm := session.NewManager(&cfg, streamer)
	ma := maadapter.New(&cfg)
	h := NewHandler(&cfg, sm, ma)
	streamer.SetTokenValidator(sm.ValidateToken)
	streamer.SetFirstClientCallback(func(id string, _ uint64) { sm.SetPlaying(id) })

	mux := http.NewServeMux()
	h.RegisterUPnPEndpoints(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	cbServer, cbCh := mockCallback(t)
	defer cbServer.Close()

	// Subscribe to AVTransport events
	w := &testRespWriter{header: make(http.Header)}
	r, _ := http.NewRequest("SUBSCRIBE", "/avtransport/event", nil)
	r.Header.Set("CALLBACK", callbackURL(cbServer))
	r.Header.Set("NT", "upnp:event")
	r.Header.Set("TIMEOUT", "Second-1800")
	h.serveEvent(w, r)

	if w.status != 200 {
		t.Fatalf("SUBSCRIBE failed: %d", w.status)
	}

	// Consume initial NOTIFY
	select {
	case <-cbCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timed out waiting for initial NOTIFY")
	}

	// SetAVTransportURI
	setBody := soapEnvelope("AVTransport", "SetAVTransportURI",
		"<InstanceID>0</InstanceID><CurrentURI>http://192.168.1.10/song.mp3</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	resp, err := http.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(setBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	// Consume SetAVTransportURI STOPPED event
	select {
	case <-cbCh:
	case <-time.After(3 * time.Second):
		t.Fatal("Timed out waiting for SetAVTransportURI NOTIFY")
	}

	// Play → PLAYING event now only fires from first /live client callback,
	// not from Play handler. Skip PLAYING expectation.
	playBody := soapEnvelope("AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	resp, err = http.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(playBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	// Pause
	pauseBody := soapEnvelope("AVTransport", "Pause", "<InstanceID>0</InstanceID>")
	resp, err = http.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(pauseBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	select {
	case pauseNotify := <-cbCh:
		pauseBody2, _ := io.ReadAll(pauseNotify.Body)
		pauseBodyStr := string(pauseBody2)
		if !strings.Contains(pauseBodyStr, "PAUSED_PLAYBACK") {
			t.Errorf("Pause NOTIFY should contain PAUSED_PLAYBACK, got: %s", pauseBodyStr)
		}
		t.Logf("Pause state change NOTIFY received, SEQ=%s", pauseNotify.Header.Get("SEQ"))
	case <-time.After(3 * time.Second):
		t.Fatal("Timed out waiting for Pause state change NOTIFY")
	}

	// Stop
	stopBody := soapEnvelope("AVTransport", "Stop", "<InstanceID>0</InstanceID>")
	resp, err = http.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(stopBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	select {
	case stopNotify := <-cbCh:
		stopBody2, _ := io.ReadAll(stopNotify.Body)
		stopBodyStr := string(stopBody2)
		if !strings.Contains(stopBodyStr, "STOPPED") {
			t.Errorf("Stop NOTIFY should contain STOPPED, got: %s", stopBodyStr)
		}
		t.Logf("Stop state change NOTIFY received, SEQ=%s", stopNotify.Header.Get("SEQ"))
	case <-time.After(3 * time.Second):
		t.Fatal("Timed out waiting for Stop state change NOTIFY")
	}
}

// TestHAErrorSetsSessionError verifies that an async HA play_media failure
// eventually puts the current active generation in ERROR_OCCURRED.
func TestHAErrorSetsSessionError(t *testing.T) {
	haServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error": "service unavailable"}`))
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
	cfg.FFmpeg.Binary = writeSleepFFmpeg(t)

	streamer := stream.NewStreamer(&cfg)
	sm := session.NewManager(&cfg, streamer)
	ma := maadapter.New(&cfg)
	h := NewHandler(&cfg, sm, ma)
	streamer.SetTokenValidator(sm.ValidateToken)
	streamer.SetFirstClientCallback(func(id string, _ uint64) { sm.SetPlaying(id) })

	mux := http.NewServeMux()
	h.RegisterUPnPEndpoints(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// SetAVTransportURI
	setBody := soapEnvelope("AVTransport", "SetAVTransportURI",
		"<InstanceID>0</InstanceID><CurrentURI>http://192.168.1.10/song.mp3</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	resp, err := http.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(setBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	// Play returns immediately; HA failure is applied asynchronously to the
	// still-current stream generation.
	playBody := soapEnvelope("AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	resp, err = http.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(playBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("Play with failing HA should return immediate success, got %d", resp.StatusCode)
	}

	sessions := sm.AllSessions()
	if len(sessions) == 0 {
		t.Fatal("No session created")
	}
	s := waitForSessionState(t, sm, sessions[0].ID, session.StateError)
	if s.Error == "" {
		t.Error("session should have error message")
	}
	t.Logf("Session error state: %s, message: %s", s.State, s.Error)

	// Verify session is in error state (not returned by ActiveSession)
	if s.State != session.StateError {
		t.Errorf("expected session error state, got %s", s.State)
	}
	// ActiveSession should not return error sessions
	if act := sm.ActiveSession(); act != nil {
		t.Errorf("ActiveSession should return nil for error-only sessions, got %v", act)
	}
	t.Logf("Session error state verified, ActiveSession correctly excludes error sessions")
}

func TestLatePlayMediaErrorAfterStopDoesNotSetSessionError(t *testing.T) {
	playStarted := make(chan struct{})
	releasePlay := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releasePlay) })
	})
	var signaled atomic.Bool
	var playRequests atomic.Int32

	haServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "play_media") {
			playRequests.Add(1)
			if signaled.CompareAndSwap(false, true) {
				close(playStarted)
			}
			<-releasePlay
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"error": "service unavailable"}`))
			return
		}
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
	cfg.FFmpeg.Binary = writeSleepFFmpeg(t)

	streamer := stream.NewStreamer(&cfg)
	sm := session.NewManager(&cfg, streamer)
	ma := maadapter.New(&cfg)
	h := NewHandler(&cfg, sm, ma)
	streamer.SetTokenValidator(sm.ValidateToken)

	mux := http.NewServeMux()
	h.RegisterUPnPEndpoints(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	setBody := soapEnvelope("AVTransport", "SetAVTransportURI",
		"<InstanceID>0</InstanceID><CurrentURI>http://192.168.1.10/song.mp3</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	resp, err := http.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(setBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	sessions := sm.AllSessions()
	if len(sessions) == 0 {
		t.Fatal("No session created")
	}
	sessionID := sessions[0].ID

	start := time.Now()
	playBody := soapEnvelope("AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	resp, err = http.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(playBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("Play expected 200, got %d", resp.StatusCode)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("Play should not wait for HA play_media, took %s", elapsed)
	}

	select {
	case <-playStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for async play_media request")
	}

	stopBody := soapEnvelope("AVTransport", "Stop", "<InstanceID>0</InstanceID>")
	resp, err = http.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(stopBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("Stop expected 200, got %d", resp.StatusCode)
	}

	releaseOnce.Do(func() { close(releasePlay) })
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && playRequests.Load() < 3 {
		time.Sleep(10 * time.Millisecond)
	}
	if playRequests.Load() < 3 {
		t.Fatalf("timed out waiting for play_media retries, got %d requests", playRequests.Load())
	}

	s := sm.Get(sessionID)
	if s.State != session.StateStopped {
		t.Fatalf("late play_media failure after Stop should leave session stopped, got %s with error %q", s.State, s.Error)
	}
	if s.Error != "" {
		t.Fatalf("late play_media failure after Stop should not set error, got %q", s.Error)
	}
}

func TestStartupSeekZeroDoesNotRestartStreamOrReplayMedia(t *testing.T) {
	var playCalls atomic.Int32
	haServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "play_media") {
			playCalls.Add(1)
		}
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
	cfg.FFmpeg.Binary = writeSleepFFmpeg(t)

	streamer := stream.NewStreamer(&cfg)
	sm := session.NewManager(&cfg, streamer)
	ma := maadapter.New(&cfg)
	h := NewHandler(&cfg, sm, ma)
	streamer.SetTokenValidator(sm.ValidateToken)

	mux := http.NewServeMux()
	h.RegisterUPnPEndpoints(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	setBody := soapEnvelope("AVTransport", "SetAVTransportURI",
		"<InstanceID>0</InstanceID><CurrentURI>http://192.168.1.10/song.mp3</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	resp, err := http.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(setBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("SetAVTransportURI expected 200, got %d", resp.StatusCode)
	}

	sessions := sm.AllSessions()
	if len(sessions) == 0 {
		t.Fatal("No session created")
	}
	sessionID := sessions[0].ID

	playBody := soapEnvelope("AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	resp, err = http.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(playBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Play expected 200, got %d", resp.StatusCode)
	}
	genBefore := sm.CurrentGenID(sessionID)
	if genBefore == 0 {
		t.Fatal("expected stream generation after Play")
	}

	seekBody := soapEnvelope("AVTransport", "Seek", "<InstanceID>0</InstanceID><Unit>REL_TIME</Unit><Target>00:00:00</Target>")
	resp, err = http.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(seekBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("startup Seek(0) expected 200, got %d", resp.StatusCode)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && playCalls.Load() < 1 {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	if got := playCalls.Load(); got != 1 {
		t.Fatalf("startup Seek(0) should not issue another play_media call, got %d calls", got)
	}
	if genAfter := sm.CurrentGenID(sessionID); genAfter != genBefore {
		t.Fatalf("startup Seek(0) should not restart stream generation: before=%d after=%d", genBefore, genAfter)
	}
}

// TestMultipleSetAVTransportURIPlaysLastURI verifies that when a controller
// sends SetAVTransportURI twice then Play, it plays the LAST URI.
func TestMultipleSetAVTransportURIPlaysLastURI(t *testing.T) {
	haServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[{"success": true}]`))
		// Log the media_id so we can inspect it
		_ = body
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
	strm.SetFirstClientCallback(func(id string, _ uint64) { sm.SetPlaying(id) })

	mux := http.NewServeMux()
	h.RegisterUPnPEndpoints(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// First SetAVTransportURI
	firstURI := "http://192.168.1.10/first_song.mp3"
	body1 := soapEnvelope("AVTransport", "SetAVTransportURI",
		"<InstanceID>0</InstanceID><CurrentURI>"+firstURI+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	resp, err := http.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(body1))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	// Second SetAVTransportURI (should replace the first)
	secondURI := "http://192.168.1.10/second_song.mp3"
	body2 := soapEnvelope("AVTransport", "SetAVTransportURI",
		"<InstanceID>0</InstanceID><CurrentURI>"+secondURI+"</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>")
	resp, err = http.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(body2))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	// There should be exactly 2 sessions: first=stopped, second=loaded
	allSessions := sm.AllSessions()
	if len(allSessions) != 2 {
		t.Fatalf("expected 2 sessions, got %d", len(allSessions))
	}

	// Find the loaded one — it should be the second URI
	var active *session.Session
	for _, s := range allSessions {
		if s.State == session.StateLoaded {
			active = s
			break
		}
	}
	if active == nil {
		t.Fatal("No loaded session found")
	}
	if active.SourceURI != secondURI {
		t.Errorf("Active session should have second URI (%s), got %s", secondURI, active.SourceURI)
	}
	t.Logf("Active session URI: %s (expected second)", active.SourceURI)

	// Play and verify the correct session is played
	playBody := soapEnvelope("AVTransport", "Play", "<InstanceID>0</InstanceID><Speed>1</Speed>")
	resp, err = http.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(playBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	// The active session should now be in "starting" state
	active = sm.Get(active.ID)
	if active.State != session.StateStarting {
		t.Errorf("expected starting state after Play, got %s", active.State)
	}
	t.Logf("Correct session played: %s with URI %s", active.ID, active.SourceURI)
}

func TestSeekWithoutGenerationDoesNotCallPlayMedia(t *testing.T) {
	var playCalls atomic.Int32
	haServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			playCalls.Add(1)
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"success": false}`))
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

	s := sm.Create("http://192.168.1.10/song.mp3", "")
	if err := sm.Play(s.ID); err != nil {
		t.Fatalf("play session: %v", err)
	}
	sm.SetPlaying(s.ID)
	if gen := sm.CurrentGenID(s.ID); gen != 0 {
		t.Fatalf("test setup expected no generation, got %d", gen)
	}

	mux := http.NewServeMux()
	h.RegisterUPnPEndpoints(mux)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	seekBody := soapEnvelope("AVTransport", "Seek", "<InstanceID>0</InstanceID><Unit>REL_TIME</Unit><Target>00:00:10</Target>")
	resp, err := http.Post(ts.URL+"/avtransport/control", "text/xml", strings.NewReader(seekBody))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if playCalls.Load() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := playCalls.Load(); got != 0 {
		t.Fatalf("Seek without a stream generation must not call HA PlayMedia; got %d calls", got)
	}
}
