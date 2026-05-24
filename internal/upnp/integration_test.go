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

// startTestServer creates a bridge HTTP server for integration testing.
func startTestServer(t *testing.T) (*httptest.Server, *Handler) {
	t.Helper()
	cfg := config.DefaultConfig()
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
			"InvalidAction",
			"/avtransport/control",
			soapEnvelope("AVTransport", "NonExistentAction", ""),
			200,
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
