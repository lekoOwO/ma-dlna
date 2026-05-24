package upnp

import (
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/leko/ma-dlna/internal/config"
)

func TestExtractSOAPAction(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		expect string
	}{
		{
			"prefixed tag",
			`<s:Envelope><s:Body><u:Play xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"></u:Play></s:Body></s:Envelope>`,
			"Play",
		},
		{
			"unprefixed tag",
			`<s:Envelope><s:Body><Play xmlns="urn:schemas-upnp-org:service:AVTransport:1"></Play></s:Body></s:Envelope>`,
			"Play",
		},
		{
			"self-closing tag",
			`<s:Envelope><s:Body><u:Stop xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"/></s:Body></s:Envelope>`,
			"Stop",
		},
		{
			"empty body",
			``,
			"",
		},
		{
			"SetAVTransportURI",
			`<s:Envelope><s:Body><u:SetAVTransportURI xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"><InstanceID>0</InstanceID><CurrentURI>http://example.com/song.mp3</CurrentURI><CurrentURIMetaData></CurrentURIMetaData></u:SetAVTransportURI></s:Body></s:Envelope>`,
			"SetAVTransportURI",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := extractSOAPAction([]byte(tc.body))
			if got != tc.expect {
				t.Errorf("expected '%s', got '%s'", tc.expect, got)
			}
		})
	}
}

func TestExtractSOAPField(t *testing.T) {
	body := `<u:SetAVTransportURI xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"><InstanceID>0</InstanceID><CurrentURI>http://example.com/song.mp3</CurrentURI><CurrentURIMetaData>&lt;DIDL-Lite&gt;test&lt;/DIDL-Lite&gt;</CurrentURIMetaData></u:SetAVTransportURI>`

	if got := extractSOAPField([]byte(body), "InstanceID"); got != "0" {
		t.Errorf("expected '0', got '%s'", got)
	}
	if got := extractSOAPField([]byte(body), "CurrentURI"); got != "http://example.com/song.mp3" {
		t.Errorf("expected song URL, got '%s'", got)
	}
	if got := extractSOAPField([]byte(body), "Nonexistent"); got != "" {
		t.Errorf("expected empty for missing field, got '%s'", got)
	}
}

func TestSOAPResponse(t *testing.T) {
	inner := `<u:PlayResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"/>`
	resp := soapResponse("", inner)

	if !strings.Contains(resp, "s:Envelope") {
		t.Error("response should contain Envelope")
	}
	if !strings.Contains(resp, "u:PlayResponse") {
		t.Error("response should contain PlayResponse")
	}
	if strings.Count(resp, "u:PlayResponse") > 1 {
		t.Error("response should NOT double-wrap PlayResponse")
	}
}

func TestSOAPFaultResponse(t *testing.T) {
	resp := soapFaultResponse("401", "Invalid Action")

	if !strings.Contains(resp, "<errorCode>401</errorCode>") {
		t.Error("fault should contain errorCode 401")
	}
	if !strings.Contains(resp, "<errorDescription>Invalid Action</errorDescription>") {
		t.Error("fault should contain error description")
	}
}

func TestEscapeXML(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"normal", "normal"},
		{"a&b", "a&amp;b"},
		{"a<b", "a&lt;b"},
		{"a>b", "a&gt;b"},
		{`a"b`, "a&quot;b"},
	}

	for _, tc := range tests {
		got := escapeXML(tc.input)
		if got != tc.expected {
			t.Errorf("escapeXML(%q): expected %q, got %q", tc.input, tc.expected, got)
		}
	}
}

func TestSSDPMessageFormat(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.UPnP.FriendlyName = "Test Bridge"
	cfg.UPnP.UUID = "uuid:test-1234"
	cfg.Server.PublicBaseURL = "http://192.168.1.10:8787"

	h := &Handler{
		cfg:        &cfg,
		deviceUUID: "uuid:test-1234",
	}

	msg := h.ssdpAliveMsg("http://192.168.1.10:8787")

	if !strings.Contains(msg, "NOTIFY * HTTP/1.1") {
		t.Error("SSDP message should start with NOTIFY")
	}
	if !strings.Contains(msg, "urn:schemas-upnp-org:device:MediaRenderer:1") {
		t.Error("SSDP should advertise as MediaRenderer:1")
	}
	if !strings.Contains(msg, "uuid:test-1234") {
		t.Error("SSDP should include UUID")
	}
	if !strings.Contains(msg, "LOCATION: http://192.168.1.10:8787/device.xml") {
		t.Error("SSDP should include device.xml location")
	}
}

func TestBaseURLForRequest(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.UPnP.AutoBaseURL = true

	h := &Handler{cfg: &cfg}

	r, _ := http.NewRequest("GET", "/device.xml", nil)
	r.Host = "10.0.0.1:8787"

	base := h.baseURLForRequest(r)
	if base != "http://10.0.0.1:8787" {
		t.Errorf("expected http://10.0.0.1:8787, got %s", base)
	}

	// Auto disabled — fall back to config
	cfg.UPnP.AutoBaseURL = false
	cfg.Server.PublicBaseURL = "http://manual.local:8787"
	base = h.baseURLForRequest(r)
	if base != "http://manual.local:8787" {
		t.Errorf("expected fallback URL, got %s", base)
	}
}

func TestBaseURLForIP(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.UPnP.AutoBaseURL = true
	cfg.Server.HTTPPort = 8787

	h := &Handler{cfg: &cfg}

	ip := net.ParseIP("192.168.1.5")
	base := h.baseURLForIP(ip)
	if base != "http://192.168.1.5:8787" {
		t.Errorf("expected per-interface URL, got %s", base)
	}

	// Auto disabled — fall back to config
	cfg.UPnP.AutoBaseURL = false
	cfg.Server.PublicBaseURL = "http://manual.local:8787"
	base = h.baseURLForIP(ip)
	if base != "http://manual.local:8787" {
		t.Errorf("expected fallback URL, got %s", base)
	}
}

func TestDeviceDescription(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.UPnP.FriendlyName = "Test Renderer"
	cfg.UPnP.Manufacturer = "Test Mfg"
	cfg.UPnP.ModelName = "Test Model"
	cfg.Server.PublicBaseURL = "http://bridge.local:8787"

	h := &Handler{
		cfg:        &cfg,
		deviceUUID: "uuid:test-1234",
	}

	w := &testRespWriter{header: make(http.Header)}
	r, _ := http.NewRequest("GET", "/device.xml", nil)
	h.serveDeviceDesc(w, r)

	body := string(w.body)
	if !strings.Contains(body, "MediaRenderer:1") {
		t.Error("device desc should contain MediaRenderer device type")
	}
	if !strings.Contains(body, "Test Renderer") {
		t.Error("device desc should contain friendly name")
	}
	if !strings.Contains(body, "AVTransport:1") {
		t.Error("device desc should list AVTransport service")
	}
	if !strings.Contains(body, "RenderingControl:1") {
		t.Error("device desc should list RenderingControl service")
	}
	if !strings.Contains(body, "ConnectionManager:1") {
		t.Error("device desc should list ConnectionManager service")
	}
}

func TestParseSOAPRequestMethodCheck(t *testing.T) {
	r, _ := http.NewRequest("GET", "/avtransport/control", nil)
	_, err := parseSOAPRequest(r)
	if err == nil {
		t.Error("GET should be rejected for SOAP endpoint")
	}
}

type testRespWriter struct {
	header http.Header
	status int
	body   []byte
}

func (w *testRespWriter) Header() http.Header {
	return w.header
}

func (w *testRespWriter) Write(b []byte) (int, error) {
	w.body = append(w.body, b...)
	return len(b), nil
}

func (w *testRespWriter) WriteHeader(status int) {
	w.status = status
}
