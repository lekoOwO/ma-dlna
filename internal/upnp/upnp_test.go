package upnp

import (
	"encoding/xml"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

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

	msg := h.ssdpAliveMsg("http://192.168.1.10:8787", "urn:schemas-upnp-org:device:MediaRenderer:1")

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

func TestParseRelTime(t *testing.T) {
	tests := []struct {
		input    string
		expected time.Duration
	}{
		{"00:00:00", 0},
		{"00:00:01", time.Second},
		{"00:01:00", time.Minute},
		{"01:00:00", time.Hour},
		{"00:00:30", 30 * time.Second},
		{"01:23:45", 1*time.Hour + 23*time.Minute + 45*time.Second},
	}
	for _, tc := range tests {
		d, err := parseRelTime(tc.input)
		if err != nil {
			t.Errorf("parseRelTime(%q) error: %v", tc.input, err)
			continue
		}
		if d != tc.expected {
			t.Errorf("parseRelTime(%q) = %v, want %v", tc.input, d, tc.expected)
		}
	}
}

func TestMatchingIPSubnetMatch(t *testing.T) {
	_, nw1, _ := net.ParseCIDR("192.168.10.0/24")
	_, nw2, _ := net.ParseCIDR("10.121.0.0/16")
	_, nw3, _ := net.ParseCIDR("172.17.0.0/16")

	networks := []ipNet{
		{ip: net.ParseIP("192.168.10.5"), nw: nw1},
		{ip: net.ParseIP("10.121.124.1"), nw: nw2},
		{ip: net.ParseIP("172.17.0.1"), nw: nw3},
	}

	// Client on 192.168.10.x → should get 192.168.10.5
	got := matchingIPWith(net.ParseIP("192.168.10.27"), networks)
	if got == nil || got.String() != "192.168.10.5" {
		t.Errorf("expected 192.168.10.5, got %v", got)
	}

	// Client on 10.121.x.x → should get 10.121.124.1
	got = matchingIPWith(net.ParseIP("10.121.124.93"), networks)
	if got == nil || got.String() != "10.121.124.1" {
		t.Errorf("expected 10.121.124.1, got %v", got)
	}

	// Client on 172.17.x.x → should get 172.17.0.1
	got = matchingIPWith(net.ParseIP("172.17.0.5"), networks)
	if got == nil || got.String() != "172.17.0.1" {
		t.Errorf("expected 172.17.0.1, got %v", got)
	}

	// Unmatched client → fall back to first network
	got = matchingIPWith(net.ParseIP("8.8.8.8"), networks)
	if got == nil || got.String() != "192.168.10.5" {
		t.Errorf("expected fallback 192.168.10.5, got %v", got)
	}
}

func TestServerString(t *testing.T) {
	s := serverString()
	if s == "" {
		t.Error("serverString must not be empty")
	}
	if !strings.Contains(s, "UPnP/1.0 dlna-ma-bridge/") {
		t.Errorf("serverString bad format: %s", s)
	}
}

func TestSSDPByeByeMessage(t *testing.T) {
	h := &Handler{deviceUUID: "uuid:test-bye"}
	msg := h.ssdpByeByeMsg("", "urn:schemas-upnp-org:device:MediaRenderer:1")
	if !strings.Contains(msg, "ssdp:byebye") {
		t.Error("byebye should contain ssdp:byebye")
	}
	if !strings.Contains(msg, "uuid:test-bye") {
		t.Error("byebye should contain UUID")
	}
}

func TestExtractST(t *testing.T) {
	tests := []struct {
		body   string
		expect string
	}{
		{"ST: upnp:rootdevice\r\n", "upnp:rootdevice"},
		{"ST: urn:schemas-upnp-org:device:MediaRenderer:1\r\n", "urn:schemas-upnp-org:device:MediaRenderer:1"},
		{"ST:ssdp:all\r\n", "ssdp:all"},
		{"NO ST HERE\r\n", ""},
	}
	for _, tc := range tests {
		got := extractST(tc.body)
		if got != tc.expect {
			t.Errorf("extractST(%q): expected %q, got %q", tc.body, tc.expect, got)
		}
	}
}

func TestMatchesSearchTarget(t *testing.T) {
	cfg := config.DefaultConfig()
	h := &Handler{cfg: &cfg, deviceUUID: "uuid:test-match"}
	tests := []struct {
		body   string
		expect bool
	}{
		{"ST: urn:schemas-upnp-org:device:MediaRenderer:1", true},
		{"ST: ssdp:all", true},
		{"ST: upnp:rootdevice", true},
		{"ST: urn:schemas-upnp-org:service:AVTransport:1", true},
		{"ST: urn:schemas-upnp-org:service:RenderingControl:1", true},
		{"ST: urn:schemas-upnp-org:service:ConnectionManager:1", true},
		{"ST: uuid:test-match", true},
		{"ST: urn:schemas-upnp-org:device:MediaServer:1", false},
		{"", false},
	}
	for _, tc := range tests {
		if got := h.matchesSearchTarget(tc.body); got != tc.expect {
			t.Errorf("matchesSearchTarget(%q): expected %v, got %v", tc.body, tc.expect, got)
		}
	}
}

func TestMServeResponseEchoST(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.Server.PublicBaseURL = "http://10.0.0.1:8080"
	cfg.UPnP.AdvertiseIntervalSecs = 1800

	h := &Handler{cfg: &cfg, deviceUUID: "uuid:test-msearch"}

	resp := h.mserveResponse("http://10.0.0.1:8080", "upnp:rootdevice")

	if !strings.Contains(resp, "ST: upnp:rootdevice") {
		t.Error("mserveResponse must echo request ST")
	}
	if !strings.Contains(resp, "USN: uuid:test-msearch::upnp:rootdevice") {
		t.Error("mserveResponse USN must contain echoed ST")
	}
	if !strings.Contains(resp, "LOCATION: http://10.0.0.1:8080/device.xml") {
		t.Error("mserveResponse must contain LOCATION")
	}
}

func TestExtractCallbackURLs(t *testing.T) {
	tests := []struct {
		input  string
		expect int
		first  string
	}{
		{"<http://192.168.1.1:12345/>", 1, "http://192.168.1.1:12345/"},
		{"<http://10.0.0.1:8000/cb> <http://10.0.0.2:8000/cb>", 2, "http://10.0.0.1:8000/cb"},
		{"", 0, ""},
		{"no-brackets", 0, ""},
	}
	for _, tc := range tests {
		urls := extractCallbackURLs(tc.input)
		if len(urls) != tc.expect {
			t.Errorf("extractCallbackURLs(%q): expected %d urls, got %d", tc.input, tc.expect, len(urls))
		}
		if tc.first != "" && (len(urls) == 0 || urls[0] != tc.first) {
			t.Errorf("extractCallbackURLs(%q): expected first url %q, got %v", tc.input, tc.first, urls)
		}
	}
}

func TestSubscriptionNew(t *testing.T) {
	cfg := config.DefaultConfig()
	h := &Handler{cfg: &cfg, deviceUUID: "uuid:test-sub"}

	w := &testRespWriter{header: make(http.Header)}
	r, _ := http.NewRequest("SUBSCRIBE", "/avtransport/event", nil)
	r.Header.Set("CALLBACK", "<http://127.0.0.1:12345/>")
	r.Header.Set("NT", "upnp:event")
	r.Header.Set("TIMEOUT", "Second-1800")
	r.Header.Set("CALLBACK", "<http://192.168.1.1:12345/>")

	h.serveEvent(w, r)

	if w.status != http.StatusOK {
		t.Errorf("new SUBSCRIBE expected 200, got %d", w.status)
	}
	sid := w.header.Get("SID")
	if !strings.HasPrefix(sid, "uuid:") {
		t.Errorf("new SUBSCRIBE SID must start with uuid:, got %s", sid)
	}
	if w.header.Get("TIMEOUT") != "Second-1800" {
		t.Error("new SUBSCRIBE must return TIMEOUT")
	}
	if w.header.Get("SERVER") == "" {
		t.Error("new SUBSCRIBE must return SERVER header")
	}
}

func TestSubscriptionRenewal(t *testing.T) {
	cfg := config.DefaultConfig()
	h := &Handler{cfg: &cfg, deviceUUID: "uuid:test-sub", subscribers: make(map[string]*eventSubscriber)}

	// First create a real subscription
	w1 := &testRespWriter{header: make(http.Header)}
	r1, _ := http.NewRequest("SUBSCRIBE", "/avtransport/event", nil)
	r1.Header.Set("CALLBACK", "<http://192.168.1.1:12345/>")
	r1.Header.Set("NT", "upnp:event")
	r1.Header.Set("TIMEOUT", "Second-1800")
	h.serveEvent(w1, r1)
	sid := w1.header.Get("SID")
	if sid == "" {
		t.Fatal("initial SUBSCRIBE must return SID")
	}

	// Renew with the real SID
	w := &testRespWriter{header: make(http.Header)}
	r, _ := http.NewRequest("SUBSCRIBE", "/avtransport/event", nil)
	r.Header.Set("SID", sid)

	h.serveEvent(w, r)

	if w.status != http.StatusOK {
		t.Errorf("renewal SUBSCRIBE expected 200, got %d", w.status)
	}
	if w.header.Get("SID") != sid {
		t.Errorf("renewal must echo existing SID, got %s", w.header.Get("SID"))
	}
	// Renewal with unknown SID should fail
	w2 := &testRespWriter{header: make(http.Header)}
	r2, _ := http.NewRequest("SUBSCRIBE", "/avtransport/event", nil)
	r2.Header.Set("SID", "uuid:nonexistent")
	h.serveEvent(w2, r2)
	if w2.status != http.StatusPreconditionFailed {
		t.Errorf("renewal with unknown SID expected 412, got %d", w2.status)
	}
}

func TestUnsubscribe(t *testing.T) {
	cfg := config.DefaultConfig()
	h := &Handler{cfg: &cfg}

	w := &testRespWriter{header: make(http.Header)}
	r, _ := http.NewRequest("UNSUBSCRIBE", "/avtransport/event", nil)
	r.Header.Set("SID", "uuid:some-sid")

	h.serveEvent(w, r)

	if w.status != http.StatusOK {
		t.Errorf("UNSUBSCRIBE expected 200, got %d", w.status)
	}
}

func TestEventInvalidMethod(t *testing.T) {
	cfg := config.DefaultConfig()
	h := &Handler{cfg: &cfg}

	w := &testRespWriter{header: make(http.Header)}
	r, _ := http.NewRequest("POST", "/avtransport/event", nil)

	h.serveEvent(w, r)

	if w.status != http.StatusMethodNotAllowed {
		t.Errorf("POST on event endpoint expected 405, got %d", w.status)
	}
}

func TestGenerateSubscriptionUUID(t *testing.T) {
	// Generate multiple UUIDs and ensure they are unique and well-formed
	uuids := make(map[string]bool)
	for i := 0; i < 10; i++ {
		u := generateSubscriptionUUID()
		if u == "" {
			t.Fatal("UUID must not be empty")
		}
		if uuids[u] {
			t.Errorf("duplicate UUID: %s", u)
		}
		uuids[u] = true
		// UUID format: 8-4-4-4-12 hex digits
		parts := strings.Split(u, "-")
		if len(parts) != 5 {
			t.Errorf("UUID %s: expected 5 dash-separated parts, got %d", u, len(parts))
		}
		if len(parts[0]) != 8 || len(parts[1]) != 4 || len(parts[2]) != 4 || len(parts[3]) != 4 || len(parts[4]) != 12 {
			t.Errorf("UUID %s: wrong segment lengths", u)
		}
	}
}

func TestDeviceDescriptionServiceURLs(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.UPnP.FriendlyName = "Test"
	cfg.Server.PublicBaseURL = "http://bridge:8787"

	h := &Handler{cfg: &cfg, deviceUUID: "uuid:test-desc"}

	w := &testRespWriter{header: make(http.Header)}
	r, _ := http.NewRequest("GET", "/device.xml", nil)
	h.serveDeviceDesc(w, r)

	body := string(w.body)
	// All service URLs should use the same base
	if !strings.Contains(body, "http://bridge:8787/service/AVTransport/desc.xml") {
		t.Error("AVTransport SCPDURL missing")
	}
	if !strings.Contains(body, "http://bridge:8787/avtransport/control") {
		t.Error("AVTransport controlURL missing")
	}
}

func TestDeviceDescriptionUsesRequestHost(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.UPnP.AutoBaseURL = true
	cfg.UPnP.FriendlyName = "Test"

	h := &Handler{cfg: &cfg, deviceUUID: "uuid:test-auto"}

	w := &testRespWriter{header: make(http.Header)}
	r, _ := http.NewRequest("GET", "/device.xml", nil)
	r.Host = "10.0.0.5:8080"
	h.serveDeviceDesc(w, r)

	body := string(w.body)
	if !strings.Contains(body, "http://10.0.0.5:8080/service/AVTransport/desc.xml") {
		t.Error("device desc should use request Host for service URLs")
	}
}

func TestMatchingIPEmpty(t *testing.T) {
	got := matchingIPWith(net.ParseIP("192.168.1.1"), nil)
	if got != nil {
		t.Errorf("expected nil for empty networks, got %v", got)
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

func TestSCPDXMLWellFormed(t *testing.T) {
	scpds := map[string]string{
		"AVTransport":       avTransportSCPD,
		"RenderingControl":  renderingControlSCPD,
		"ConnectionManager": connectionManagerSCPD,
	}
	for name, scpdXML := range scpds {
		t.Run(name, func(t *testing.T) {
			decoder := xml.NewDecoder(strings.NewReader(scpdXML))
			depth := 0
			for {
				tok, err := decoder.Token()
				if err != nil {
					break
				}
				switch tok.(type) {
				case xml.StartElement:
					depth++
				case xml.EndElement:
					depth--
					if depth < 0 {
						t.Error("unmatched closing tag")
					}
				}
			}
			if depth != 0 {
				t.Errorf("unclosed tags: final depth=%d", depth)
			}
		})
	}
}

func TestSCPDHasRequiredActions(t *testing.T) {
	// AVTransport must declare all implemented actions
	avtActions := []string{
		"SetAVTransportURI", "Play", "Stop", "Pause",
		"GetTransportInfo", "GetPositionInfo", "GetMediaInfo",
		"Next", "Previous", "SetNextAVTransportURI", "SetPlayMode",
		"Seek", "GetCurrentTransportActions", "GetDeviceCapabilities",
		"GetTransportSettings",
	}
	for _, a := range avtActions {
		if !strings.Contains(avTransportSCPD, "<name>"+a+"</name>") {
			t.Errorf("AVTransport SCPD missing: %s", a)
		}
	}

	// RCS should only have volume/mute
	rcsActions := []string{"GetVolume", "SetVolume", "GetMute", "SetMute"}
	for _, a := range rcsActions {
		if !strings.Contains(renderingControlSCPD, "<name>"+a+"</name>") {
			t.Errorf("RenderingControl SCPD missing: %s", a)
		}
	}
	for _, a := range avtActions {
		if strings.Contains(renderingControlSCPD, "<name>"+a+"</name>") {
			t.Errorf("RenderingControl SCPD leaked AVTransport action: %s", a)
		}
	}

	// CM should only have connection actions
	cmActions := []string{"GetProtocolInfo", "GetCurrentConnectionIDs", "GetCurrentConnectionInfo"}
	for _, a := range cmActions {
		if !strings.Contains(connectionManagerSCPD, "<name>"+a+"</name>") {
			t.Errorf("ConnectionManager SCPD missing: %s", a)
		}
	}
	for _, a := range avtActions {
		if strings.Contains(connectionManagerSCPD, "<name>"+a+"</name>") {
			t.Errorf("ConnectionManager SCPD leaked AVTransport action: %s", a)
		}
	}
}

func TestExtractSOAPActionNamespaceVariants(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		expect string
	}{
		{
			"soap-env prefix",
			`<soap-env:Envelope xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/"><soap-env:Body><u:Play xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"></u:Play></soap-env:Body></soap-env:Envelope>`,
			"Play",
		},
		{
			"SOAP-ENV prefix",
			`<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/"><SOAP-ENV:Body><u:Stop xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"/></SOAP-ENV:Body></SOAP-ENV:Envelope>`,
			"Stop",
		},
		{
			"default namespace on envelope",
			`<Envelope xmlns="http://schemas.xmlsoap.org/soap/envelope/"><Body><u:SetAVTransportURI xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"><InstanceID>0</InstanceID></u:SetAVTransportURI></Body></Envelope>`,
			"SetAVTransportURI",
		},
		{
			"no namespace on action",
			`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body><Play>0</Play></s:Body></s:Envelope>`,
			"Play",
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

func TestSSDPAliveMessageFields(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.UPnP.AdvertiseIntervalSecs = 1800
	h := &Handler{cfg: &cfg, deviceUUID: "uuid:test-ssdp"}

	targets := []string{
		"upnp:rootdevice",
		"uuid:test-ssdp",
		"urn:schemas-upnp-org:device:MediaRenderer:1",
		"urn:schemas-upnp-org:service:AVTransport:1",
		"urn:schemas-upnp-org:service:RenderingControl:1",
		"urn:schemas-upnp-org:service:ConnectionManager:1",
	}

	for _, nt := range targets {
		t.Run("target="+nt, func(t *testing.T) {
			msg := h.ssdpAliveMsg("http://192.168.1.1:8787", nt)
			if !strings.Contains(msg, "NOTIFY * HTTP/1.1") {
				t.Error("missing NOTIFY start line")
			}
			if !strings.Contains(msg, "HOST: 239.255.255.250:1900") {
				t.Error("missing HOST header")
			}
			if !strings.Contains(msg, "NTS: ssdp:alive") {
				t.Error("missing NTS: ssdp:alive")
			}
			if !strings.Contains(msg, "NT: "+nt) {
				t.Errorf("missing NT: %s", nt)
			}
			expectedUSN := "uuid:test-ssdp::" + nt
			if strings.HasPrefix(nt, "uuid:") {
				expectedUSN = nt
			}
			if !strings.Contains(msg, "USN: "+expectedUSN) {
				t.Errorf("missing USN for target %s, expected USN: %s", nt, expectedUSN)
			}
			if !strings.Contains(msg, "LOCATION: http://192.168.1.1:8787/device.xml") {
				t.Error("missing LOCATION header")
			}
			if !strings.Contains(msg, "CACHE-CONTROL: max-age=1800") {
				t.Error("missing CACHE-CONTROL")
			}
			if !strings.Contains(msg, "SERVER:") {
				t.Error("missing SERVER header")
			}
		})
	}
}

func TestByeByeMessageFields(t *testing.T) {
	h := &Handler{deviceUUID: "uuid:test-bye"}
	msg := h.ssdpByeByeMsg("", "urn:schemas-upnp-org:device:MediaRenderer:1")

	if !strings.Contains(msg, "NTS: ssdp:byebye") {
		t.Error("byebye missing NTS")
	}
	if !strings.Contains(msg, "NT: urn:schemas-upnp-org:device:MediaRenderer:1") {
		t.Error("byebye missing NT")
	}
	if !strings.Contains(msg, "USN: uuid:test-bye::urn:schemas-upnp-org:device:MediaRenderer:1") {
		t.Error("byebye missing USN")
	}
	if strings.Contains(msg, "LOCATION") {
		t.Error("byebye should NOT contain LOCATION")
	}
	if strings.Contains(msg, "CACHE-CONTROL") {
		t.Error("byebye should NOT contain CACHE-CONTROL")
	}
}

func TestIdleDebounceFirstIdleStartsTimer(t *testing.T) {
	now := time.Now()
	d := &idleDebounce{wasPlaying: true}

	shouldStop, event := d.update("idle", now)
	if shouldStop {
		t.Error("first idle should not stop")
	}
	if event != "" {
		t.Errorf("first idle should not emit event, got %q", event)
	}
	if d.idleSince.IsZero() {
		t.Error("first idle should set idleSince")
	}
}

func TestIdleDebounceBeforeDeadlineDoesNotStop(t *testing.T) {
	d := &idleDebounce{wasPlaying: true}
	now := time.Now()

	// First idle starts timer
	d.update("idle", now)

	// 5 seconds later (before 10s debounce), still idle → should not stop
	shouldStop, _ := d.update("idle", now.Add(5*time.Second))
	if shouldStop {
		t.Error("idle before debounce duration should not stop even after long monitor runtime")
	}
}

func TestIdleDebounceAfterDeadlineStops(t *testing.T) {
	d := &idleDebounce{wasPlaying: true}
	now := time.Now()

	// First idle starts timer
	d.update("idle", now)

	// 10 seconds later (at debounce duration), still idle → should stop
	shouldStop, event := d.update("idle", now.Add(10*time.Second))
	if !shouldStop {
		t.Error("idle at debounce duration should stop")
	}
	if event != "STOPPED" {
		t.Errorf("expected STOPPED event, got %q", event)
	}
}

func TestIdleDebouncePlayingResetsIdle(t *testing.T) {
	d := &idleDebounce{wasPlaying: true}
	now := time.Now()

	// First idle starts timer
	d.update("idle", now)

	// Then playing → resets idle tracking
	shouldStop, _ := d.update("playing", now.Add(3*time.Second))
	if shouldStop {
		t.Error("playing should not stop")
	}
	if !d.idleSince.IsZero() {
		t.Error("playing should reset idleSince")
	}
	if !d.wasPlaying {
		t.Error("playing should set wasPlaying to true")
	}

	// Now idle again at much later time → starts fresh timer, does NOT stop immediately
	later := now.Add(50 * time.Second)
	shouldStop, _ = d.update("idle", later)
	if shouldStop {
		t.Error("first idle after playing should not stop immediately even after long monitor runtime")
	}
	if !d.idleSince.Equal(later) {
		t.Errorf("idleSince should be %v, got %v", later, d.idleSince)
	}
}

func TestIdleDebouncePausedResetsIdleAndEmitsOnce(t *testing.T) {
	d := &idleDebounce{wasPlaying: true}
	now := time.Now()

	// First idle starts timer
	d.update("idle", now)

	// Paused resets idle tracking and emits PAUSED_PLAYBACK
	shouldStop, event := d.update("paused", now.Add(3*time.Second))
	if shouldStop {
		t.Error("paused should not stop")
	}
	if event != "PAUSED_PLAYBACK" {
		t.Errorf("expected PAUSED_PLAYBACK, got %q", event)
	}
	if !d.idleSince.IsZero() {
		t.Error("paused should reset idleSince")
	}

	// Second paused should not re-emit
	_, event = d.update("paused", now.Add(4*time.Second))
	if event != "" {
		t.Errorf("second paused should not emit, got %q", event)
	}
}

func TestIdleDebounceOffAndStandbyBehaveLikeIdle(t *testing.T) {
	now := time.Now()

	for _, state := range []string{"off", "standby"} {
		d2 := &idleDebounce{wasPlaying: true}
		d2.update(state, now)
		// Should start idle tracking
		if d2.idleSince.IsZero() {
			t.Errorf("%s should set idleSince", state)
		}
		// After debounce should stop
		shouldStop, event := d2.update(state, now.Add(10*time.Second))
		if !shouldStop {
			t.Errorf("%s at debounce duration should stop", state)
		}
		if event != "STOPPED" {
			t.Errorf("%s should emit STOPPED, got %q", state, event)
		}
	}
}

func TestIdleDebounceNonEndedStateResetsIdle(t *testing.T) {
	now := time.Now()
	d := &idleDebounce{wasPlaying: true}

	// idle at t0 sets idleSince
	d.update("idle", now)
	if d.idleSince.IsZero() {
		t.Fatal("first idle should set idleSince")
	}

	// buffering at t0+5 should reset idle tracking without emitting
	shouldStop, event := d.update("buffering", now.Add(5*time.Second))
	if shouldStop {
		t.Error("buffering should not stop")
	}
	if event != "" {
		t.Errorf("buffering should not emit event, got %q", event)
	}
	if !d.idleSince.IsZero() {
		t.Error("buffering should reset idleSince")
	}

	// idle at t0+20 starts a fresh timer, must not stop immediately
	shouldStop, event = d.update("idle", now.Add(20*time.Second))
	if shouldStop {
		t.Error("first idle after buffering should not stop (fresh timer)")
	}
	if event != "" {
		t.Errorf("first idle after buffering should not emit, got %q", event)
	}
	// idleSince should be the new "now" (t0+20), not the original t0
	if !d.idleSince.Equal(now.Add(20 * time.Second)) {
		t.Errorf("idleSince should be t0+20, got %v", d.idleSince)
	}
}

func TestUUIDUSNNormalization(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"uuid:test-device", "uuid:test-device"},
		{"test-device", "uuid:test-device"},
		{"uuid:uuid:test-device", "uuid:test-device"},
		{"uuid:", "uuid:"},
		{"", "uuid:"},
	}
	for _, tc := range tests {
		got := uuidUSN(tc.input)
		if got != tc.expected {
			t.Errorf("uuidUSN(%q) = %q, want %q", tc.input, got, tc.expected)
		}
	}
}

func TestNotifySubscribersSerializesPerSubscriberDelivery(t *testing.T) {
	var mu sync.Mutex
	var order []string
	done := make(chan struct{}, 1)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seq := r.Header.Get("SEQ")
		if seq == "1" {
			time.Sleep(100 * time.Millisecond)
		}
		mu.Lock()
		order = append(order, seq)
		n := len(order)
		mu.Unlock()
		if n >= 2 {
			select {
			case done <- struct{}{}:
			default:
			}
		}
	}))
	defer srv.Close()

	h := &Handler{
		deviceUUID: "uuid:test-order",
		subscribers: map[string]*eventSubscriber{
			"uuid:ordered": {
				sid:       "uuid:ordered",
				callback:  "<" + srv.URL + ">",
				service:   "AVTransport",
				seq:       0,
				expiresAt: time.Now().Add(time.Hour),
			},
		},
	}

	h.notifySubscribers("AVTransport", avTransportLastChange("STOPPED"))
	h.notifySubscribers("AVTransport", avTransportLastChange("PLAYING"))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for two callback requests")
	}

	mu.Lock()
	got := order
	mu.Unlock()

	if len(got) != 2 {
		t.Fatalf("expected 2 deliveries, got %d: %v", len(got), got)
	}
	for i, want := range []string{"1", "2"} {
		if got[i] != want {
			t.Errorf("position %d: expected SEQ %q, got %q. Order: %v", i, want, got[i], got)
		}
	}
}

func TestEventSubscriberExpiryRemovesSubscriber(t *testing.T) {
	h := &Handler{
		subscribers: make(map[string]*eventSubscriber),
	}
	sub := &eventSubscriber{
		sid:       "uuid:expire",
		callback:  "<http://127.0.0.1:1/callback>",
		service:   "AVTransport",
		expiresAt: time.Now().Add(20 * time.Millisecond),
	}

	h.mu.Lock()
	h.subscribers[sub.sid] = sub
	h.enqueueSubscriberDeliveryLocked(sub, eventDelivery{
		callback: sub.callback,
		sid:      sub.sid,
		service:  sub.service,
		seq:      1,
		body:     "<event/>",
	})
	h.scheduleExpiryLocked(sub, sub.sid)
	h.mu.Unlock()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		h.mu.RLock()
		_, exists := h.subscribers[sub.sid]
		h.mu.RUnlock()
		if !exists {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	h.mu.RLock()
	_, exists := h.subscribers[sub.sid]
	h.mu.RUnlock()
	if exists {
		t.Fatal("expired subscriber was not removed")
	}
	sub.queueMu.Lock()
	closed := sub.closed
	sub.queueMu.Unlock()
	if !closed {
		t.Fatal("expired subscriber queue was not closed")
	}
}

func TestSubscribeInitialEventPrecedesStateChange(t *testing.T) {
	var mu sync.Mutex
	var order []string
	done := make(chan struct{}, 1)

	cbServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		order = append(order, r.Header.Get("SEQ"))
		n := len(order)
		mu.Unlock()
		if n >= 2 {
			select {
			case done <- struct{}{}:
			default:
			}
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer cbServer.Close()

	cfg := config.DefaultConfig()
	cfg.Security.AllowLoopbackSources = true
	h := NewHandler(&cfg, nil, nil)

	w := httptest.NewRecorder()
	r := httptest.NewRequest("SUBSCRIBE", "/avtransport/event", nil)
	r.RemoteAddr = "127.0.0.1:12345"
	r.Header.Set("CALLBACK", "<"+cbServer.URL+">")
	r.Header.Set("NT", "upnp:event")
	r.Header.Set("TIMEOUT", "Second-1800")

	h.serveEvent(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("SUBSCRIBE returned %d", w.Code)
	}

	h.notifySubscribers("AVTransport", avTransportLastChange("PLAYING"))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for initial and state-change events")
	}

	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("expected 2 deliveries, got %d: %v", len(got), got)
	}
	if got[0] != "0" || got[1] != "1" {
		t.Fatalf("expected SEQ order [0 1], got %v", got)
	}
}
