package upnp

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/leko/ma-dlna/internal/config"
	"github.com/leko/ma-dlna/internal/maadapter"
	"github.com/leko/ma-dlna/internal/session"
)

const bridgeVersion = "0.1.0"

func serverString() string {
	return runtime.GOOS + "/ UPnP/1.0 dlna-ma-bridge/" + bridgeVersion
}

type Handler struct {
	cfg        *config.Config
	sessionMgr *session.Manager
	maAdapter  *maadapter.Adapter
	mu         sync.RWMutex
	volume     int
	muted      bool
	ssdpCancel context.CancelFunc
	deviceUUID string
}

func NewHandler(cfg *config.Config, sessionMgr *session.Manager, maAdapter *maadapter.Adapter) *Handler {
	return &Handler{
		cfg:        cfg,
		sessionMgr: sessionMgr,
		maAdapter:  maAdapter,
		volume:     50,
		deviceUUID: cfg.UPnP.UUID,
	}
}

func (h *Handler) Start(ctx context.Context) error {
	ctx, h.ssdpCancel = context.WithCancel(ctx)
	go h.ssdpLoop(ctx)
	go h.mserve(ctx)
	slog.Info("UPnP handler started", "friendly_name", h.cfg.UPnP.FriendlyName, "uuid", h.deviceUUID)
	return nil
}

func (h *Handler) Stop() {
	if h.ssdpCancel != nil {
		h.ssdpCancel()
	}
	slog.Info("UPnP handler stopped")
}

func (h *Handler) RegisterUPnPEndpoints(mux *http.ServeMux) {
	mux.HandleFunc("/device.xml", h.serveDeviceDesc)
	mux.HandleFunc("/avtransport/", h.serveAVTransport)
	mux.HandleFunc("/rendering/", h.serveRenderingControl)
	mux.HandleFunc("/connection/", h.serveConnectionManager)
	mux.HandleFunc("/service/AVTransport/desc.xml", h.serveAVTransportDesc)
	mux.HandleFunc("/service/RenderingControl/desc.xml", h.serveRenderingControlDesc)
	mux.HandleFunc("/service/ConnectionManager/desc.xml", h.serveConnectionManagerDesc)
}

// ---- Base URL helpers ----

func (h *Handler) baseURLForRequest(r *http.Request) string {
	if h.cfg.UPnP.AutoBaseURL && r.Host != "" {
		return "http://" + r.Host
	}
	return h.cfg.Server.PublicBaseURL
}

func (h *Handler) baseURLForIP(ip net.IP) string {
	if h.cfg.UPnP.AutoBaseURL && ip != nil {
		return fmt.Sprintf("http://%s:%d", ip.String(), h.cfg.Server.HTTPPort)
	}
	return h.cfg.Server.PublicBaseURL
}

// ---- Multicast helpers ----

var ssdpAddr = &net.UDPAddr{IP: net.IPv4(239, 255, 255, 250), Port: 1900}

func multicastInterfaces() []net.Interface {
	var out []net.Interface
	ifaces, err := net.Interfaces()
	if err != nil {
		return out
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagMulticast == 0 {
			continue
		}
		addrs, _ := iface.Addrs()
		for _, a := range addrs {
			if ipn, ok := a.(*net.IPNet); ok && ipn.IP.To4() != nil && !ipn.IP.IsLoopback() {
				out = append(out, iface)
				break
			}
		}
	}
	return out
}

func firstIPv4(iface net.Interface) net.IP {
	addrs, err := iface.Addrs()
	if err != nil {
		return nil
	}
	for _, a := range addrs {
		if ipn, ok := a.(*net.IPNet); ok && ipn.IP.To4() != nil && !ipn.IP.IsLoopback() {
			return ipn.IP
		}
	}
	return nil
}

// localIPNetworks returns all (IP, *net.IPNet) for non-loopback IPv4 addresses.
func localIPNetworks() []ipNet {
	var out []ipNet
	ifaces, _ := net.Interfaces()
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		addrs, _ := iface.Addrs()
		for _, a := range addrs {
			ipn, ok := a.(*net.IPNet)
			if !ok || ipn.IP.To4() == nil || ipn.IP.IsLoopback() {
				continue
			}
			out = append(out, ipNet{ip: ipn.IP, nw: ipn})
		}
	}
	return out
}

type ipNet struct {
	ip net.IP
	nw *net.IPNet
}

// matchingIP finds a local IPv4 address that shares a subnet with remote.
// The networks parameter allows injection for testing.
func matchingIPWith(remote net.IP, networks []ipNet) net.IP {
	for _, n := range networks {
		if n.nw.Contains(remote) {
			return n.ip
		}
	}
	for _, n := range networks {
		return n.ip
	}
	return nil
}

func matchingIP(remote net.IP) net.IP {
	return matchingIPWith(remote, localIPNetworks())
}

// ---- M-SEARCH ----

type msearchKey struct {
	ip string
	st string
}

func (h *Handler) mserve(ctx context.Context) {
	conns := make([]*net.UDPConn, 0)
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()

	for _, iface := range multicastInterfaces() {
		conn, err := net.ListenMulticastUDP("udp4", &iface, ssdpAddr)
		if err != nil {
			slog.Warn("M-SEARCH listen failed", "iface", iface.Name, "error", err)
			continue
		}
		conns = append(conns, conn)
	}

	if len(conns) == 0 {
		slog.Warn("No multicast interfaces available for M-SEARCH")
		return
	}
	slog.Info("M-SEARCH listening", "interfaces", len(conns))

	type msg struct {
		data       []byte
		remoteAddr *net.UDPAddr
		conn       *net.UDPConn
	}
	ch := make(chan msg, 8)

	for _, conn := range conns {
		go func(c *net.UDPConn) {
			buf := make([]byte, 4096)
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				c.SetReadDeadline(time.Now().Add(time.Second))
				n, remoteAddr, err := c.ReadFromUDP(buf)
				if err != nil {
					if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
						continue
					}
					if ctx.Err() != nil {
						return
					}
					continue
				}
				data := make([]byte, n)
				copy(data, buf[:n])
				ch <- msg{data: data, remoteAddr: remoteAddr, conn: c}
			}
		}(conn)
	}

	history := map[msearchKey]time.Time{}
	cleanupTicker := time.NewTicker(time.Minute)
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-cleanupTicker.C:
			now := time.Now()
			for k, t := range history {
				if now.Sub(t) > time.Minute {
					delete(history, k)
				}
			}
		case m := <-ch:
			body := string(m.data)
			if !strings.Contains(body, "M-SEARCH") {
				continue
			}
			st := extractST(body)
			if !matchesSearchTarget(body) {
				continue
			}
			key := msearchKey{ip: m.remoteAddr.IP.String(), st: st}
			if _, exists := history[key]; exists {
				continue
			}
			history[key] = time.Now()

			localIP := matchingIP(m.remoteAddr.IP)
			slog.Debug("M-SEARCH responded", "from", m.remoteAddr.String(), "st", st, "local_ip", localIP)
			resp := h.mserveResponse(h.baseURLForIP(localIP), st)
			m.conn.WriteToUDP([]byte(resp), m.remoteAddr)
		}
	}
}

func extractST(body string) string {
	for _, line := range strings.Split(body, "\r\n") {
		if strings.HasPrefix(strings.ToUpper(line), "ST:") {
			return strings.TrimSpace(line[3:])
		}
	}
	return ""
}

func matchesSearchTarget(body string) bool {
	for _, st := range []string{
		"urn:schemas-upnp-org:device:MediaRenderer:1",
		"ssdp:all",
		"upnp:rootdevice",
		"urn:schemas-upnp-org:service:AVTransport:1",
	} {
		if strings.Contains(body, st) {
			return true
		}
	}
	return false
}

func (h *Handler) mserveResponse(base, st string) string {
	return fmt.Sprintf(
		"HTTP/1.1 200 OK\r\n"+
			"CACHE-CONTROL: max-age=%d\r\n"+
			"EXT:\r\n"+
			"LOCATION: %s/device.xml\r\n"+
			"SERVER: %s\r\n"+
			"ST: %s\r\n"+
			"USN: %s::%s\r\n"+
			"\r\n",
		h.cfg.UPnP.AdvertiseIntervalSecs,
		base,
		serverString(),
		st,
		h.deviceUUID,
		st,
	)
}

// ---- SSDP Advertisement ----

func (h *Handler) ssdpLoop(ctx context.Context) {
	interval := time.Duration(h.cfg.UPnP.AdvertiseIntervalSecs) * time.Second
	if interval <= 0 {
		interval = 30 * time.Minute
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	h.broadcastSSDP(h.ssdpAliveMsg)

	for {
		select {
		case <-ctx.Done():
			h.broadcastSSDP(h.ssdpByeByeMsg)
			return
		case <-ticker.C:
			h.broadcastSSDP(h.ssdpAliveMsg)
		}
	}
}

func (h *Handler) broadcastSSDP(msgFn func(string) string) {
	ifaces := multicastInterfaces()
	slog.Debug("SSDP broadcast", "interfaces", len(ifaces))
	for _, iface := range ifaces {
		ip := firstIPv4(iface)
		if ip == nil {
			continue
		}
		conn, err := net.DialUDP("udp4", &net.UDPAddr{IP: ip}, ssdpAddr)
		if err != nil {
			slog.Warn("SSDP dial failed", "iface", iface.Name, "ip", ip, "error", err)
			continue
		}
		conn.Write([]byte(msgFn(h.baseURLForIP(ip))))
		conn.Close()
	}
}

func (h *Handler) ssdpAliveMsg(base string) string {
	return fmt.Sprintf(
		"NOTIFY * HTTP/1.1\r\n"+
			"HOST: 239.255.255.250:1900\r\n"+
			"CACHE-CONTROL: max-age=%d\r\n"+
			"LOCATION: %s/device.xml\r\n"+
			"NT: %s\r\n"+
			"NTS: ssdp:alive\r\n"+
			"SERVER: %s\r\n"+
			"USN: %s::urn:schemas-upnp-org:device:MediaRenderer:1\r\n"+
			"\r\n",
		h.cfg.UPnP.AdvertiseIntervalSecs,
		base,
		serverString(),
		"urn:schemas-upnp-org:device:MediaRenderer:1",
		h.deviceUUID,
	)
}

func (h *Handler) ssdpByeByeMsg(_ string) string {
	return fmt.Sprintf(
		"NOTIFY * HTTP/1.1\r\n"+
			"HOST: 239.255.255.250:1900\r\n"+
			"NT: urn:schemas-upnp-org:device:MediaRenderer:1\r\n"+
			"NTS: ssdp:byebye\r\n"+
			"USN: %s::urn:schemas-upnp-org:device:MediaRenderer:1\r\n"+
			"\r\n",
		h.deviceUUID,
	)
}

// ---- Device Description XML ----

func (h *Handler) serveDeviceDesc(w http.ResponseWriter, r *http.Request) {
	base := h.baseURLForRequest(r)
	slog.Debug("Device description served", "remote", r.RemoteAddr, "base", base)
	xml := fmt.Sprintf(`<?xml version="1.0"?>
<root xmlns="urn:schemas-upnp-org:device-1-0">
  <specVersion>
    <major>1</major>
    <minor>0</minor>
  </specVersion>
  <device>
    <deviceType>urn:schemas-upnp-org:device:MediaRenderer:1</deviceType>
    <friendlyName>%s</friendlyName>
    <manufacturer>%s</manufacturer>
    <modelName>%s</modelName>
    <UDN>%s</UDN>
    <serviceList>
      <service>
        <serviceType>urn:schemas-upnp-org:service:AVTransport:1</serviceType>
        <serviceId>urn:upnp-org:serviceId:AVTransport</serviceId>
        <SCPDURL>%s/service/AVTransport/desc.xml</SCPDURL>
        <controlURL>%s/avtransport/control</controlURL>
        <eventSubURL>%s/avtransport/event</eventSubURL>
      </service>
      <service>
        <serviceType>urn:schemas-upnp-org:service:RenderingControl:1</serviceType>
        <serviceId>urn:upnp-org:serviceId:RenderingControl</serviceId>
        <SCPDURL>%s/service/RenderingControl/desc.xml</SCPDURL>
        <controlURL>%s/rendering/control</controlURL>
        <eventSubURL>%s/rendering/event</eventSubURL>
      </service>
      <service>
        <serviceType>urn:schemas-upnp-org:service:ConnectionManager:1</serviceType>
        <serviceId>urn:upnp-org:serviceId:ConnectionManager</serviceId>
        <SCPDURL>%s/service/ConnectionManager/desc.xml</SCPDURL>
        <controlURL>%s/connection/control</controlURL>
        <eventSubURL>%s/connection/event</eventSubURL>
      </service>
    </serviceList>
  </device>
</root>`,
		h.cfg.UPnP.FriendlyName,
		h.cfg.UPnP.Manufacturer,
		h.cfg.UPnP.ModelName,
		h.deviceUUID,
		base, base, base,
		base, base, base,
		base, base, base,
	)

	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.Write([]byte(xml))
}

// ---- AVTransport ----

func (h *Handler) serveAVTransport(w http.ResponseWriter, r *http.Request) {
	body, err := parseSOAPRequest(r)
	if err != nil {
		http.Error(w, "Bad request", 400)
		return
	}

	action := extractSOAPAction(body)

	slog.Debug("AVTransport SOAP request", "body", string(body))
	slog.Info("AVTransport action", "action", action)

	var response string

	switch action {
	case "SetAVTransportURI":
		instanceID := extractSOAPField(body, "InstanceID")
		uri := extractSOAPField(body, "CurrentURI")
		metadata := extractSOAPField(body, "CurrentURIMetaData")

		slog.Info("SetAVTransportURI", "uri", safeURL(uri), "instance_id", instanceID)
		h.sessionMgr.Create(uri, metadata)
		response = avTransportResponse(action, fmt.Sprintf(`
<u:SetAVTransportURIResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"/>`))
		_ = instanceID

	case "Play":
		instanceID := extractSOAPField(body, "InstanceID")
		active := h.sessionMgr.ActiveSession()
		if active != nil {
			slog.Info("Play requested, calling MA", "entity", h.cfg.HA.TargetEntityID, "stream_url", active.StreamURL)
			h.sessionMgr.Play(active.ID)
			h.maAdapter.PlayMedia(
				h.cfg.HA.TargetEntityID,
				active.StreamURL,
				"music",
			)
		} else {
			slog.Warn("Play with no active session")
		}
		response = avTransportResponse(action, fmt.Sprintf(`
<u:PlayResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"/>`))
		_ = instanceID

	case "Stop":
		instanceID := extractSOAPField(body, "InstanceID")
		active := h.sessionMgr.ActiveSession()
		if active != nil {
			h.sessionMgr.Stop(active.ID)
			h.maAdapter.Stop(h.cfg.HA.TargetEntityID)
		}
		response = avTransportResponse(action, fmt.Sprintf(`
<u:StopResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"/>`))
		_ = instanceID

	case "Pause":
		instanceID := extractSOAPField(body, "InstanceID")
		active := h.sessionMgr.ActiveSession()
		if active != nil {
			h.sessionMgr.Pause(active.ID)
			h.maAdapter.Pause(h.cfg.HA.TargetEntityID)
		}
		response = avTransportResponse(action, fmt.Sprintf(`
<u:PauseResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"/>`))
		_ = instanceID

	case "GetTransportInfo":
		state := "STOPPED"
		status := "OK"
		active := h.sessionMgr.ActiveSession()
		if active != nil {
			switch active.State {
			case session.StatePlaying:
				state = "PLAYING"
			case session.StatePaused:
				state = "PAUSED_PLAYBACK"
			case session.StateStarting:
				state = "TRANSITIONING"
			case session.StateStopped:
				state = "STOPPED"
			default:
				state = "STOPPED"
			}
		}
		response = avTransportResponse(action, fmt.Sprintf(`
<u:GetTransportInfoResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1">
  <CurrentTransportState>%s</CurrentTransportState>
  <CurrentTransportStatus>%s</CurrentTransportStatus>
  <CurrentSpeed>1</CurrentSpeed>
</u:GetTransportInfoResponse>`, state, status))

	case "GetPositionInfo":
		response = avTransportResponse(action, `
<u:GetPositionInfoResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1">
  <Track>1</Track>
  <TrackDuration>00:00:00</TrackDuration>
  <TrackMetaData></TrackMetaData>
  <TrackURI></TrackURI>
  <RelTime>00:00:00</RelTime>
  <AbsTime>00:00:00</AbsTime>
  <RelCount>2147483647</RelCount>
  <AbsCount>2147483647</AbsCount>
</u:GetPositionInfoResponse>`)

	case "GetMediaInfo":
		active := h.sessionMgr.ActiveSession()
		uri := ""
		if active != nil {
			uri = active.StreamURL
		}
		response = avTransportResponse(action, fmt.Sprintf(`
<u:GetMediaInfoResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1">
  <NrTracks>1</NrTracks>
  <MediaDuration>00:00:00</MediaDuration>
  <CurrentURI>%s</CurrentURI>
  <CurrentURIMetaData></CurrentURIMetaData>
  <NextURI></NextURI>
  <NextURIMetaData></NextURIMetaData>
  <PlayMedium>NETWORK</PlayMedium>
  <RecordMedium>NOT_IMPLEMENTED</RecordMedium>
  <WriteStatus>NOT_IMPLEMENTED</WriteStatus>
</u:GetMediaInfoResponse>`, escapeXML(uri)))

	default:
		slog.Warn("Unknown AVTransport action", "action", action)
		response = soapFaultResponse("401", "Invalid Action")
	}

	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.Write([]byte(response))
}

// ---- RenderingControl ----

func (h *Handler) serveRenderingControl(w http.ResponseWriter, r *http.Request) {
	body, err := parseSOAPRequest(r)
	if err != nil {
		http.Error(w, "Bad request", 400)
		return
	}

	action := extractSOAPAction(body)
	slog.Debug("RenderingControl action", "action", action)

	var response string

	switch action {
	case "GetVolume":
		h.mu.RLock()
		vol := h.volume
		h.mu.RUnlock()
		response = renderingResponse(action, fmt.Sprintf(`
<u:GetVolumeResponse xmlns:u="urn:schemas-upnp-org:service:RenderingControl:1">
  <CurrentVolume>%d</CurrentVolume>
</u:GetVolumeResponse>`, vol))

	case "SetVolume":
		desired := extractSOAPField(body, "DesiredVolume")
		vol := 50
		if _, err := fmt.Sscanf(desired, "%d", &vol); err != nil {
			vol = 50
		}

		h.mu.Lock()
		h.volume = vol
		h.mu.Unlock()

		h.maAdapter.SetVolume(h.cfg.HA.TargetEntityID, vol)

		response = renderingResponse(action, `
<u:SetVolumeResponse xmlns:u="urn:schemas-upnp-org:service:RenderingControl:1"/>`)

	case "GetMute":
		h.mu.RLock()
		muted := h.muted
		h.mu.RUnlock()
		muteStr := "0"
		if muted {
			muteStr = "1"
		}
		response = renderingResponse(action, fmt.Sprintf(`
<u:GetMuteResponse xmlns:u="urn:schemas-upnp-org:service:RenderingControl:1">
  <CurrentMute>%s</CurrentMute>
</u:GetMuteResponse>`, muteStr))

	case "SetMute":
		muteStr := extractSOAPField(body, "DesiredMute")
		h.mu.Lock()
		h.muted = muteStr == "1"
		h.mu.Unlock()

		response = renderingResponse(action, `
<u:SetMuteResponse xmlns:u="urn:schemas-upnp-org:service:RenderingControl:1"/>`)

	default:
		response = soapFaultResponse("401", "Invalid Action")
	}

	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.Write([]byte(response))
}

// ---- ConnectionManager ----

func (h *Handler) serveConnectionManager(w http.ResponseWriter, r *http.Request) {
	body, err := parseSOAPRequest(r)
	if err != nil {
		http.Error(w, "Bad request", 400)
		return
	}

	action := extractSOAPAction(body)
	slog.Debug("ConnectionManager action", "action", action)

	var response string

	switch action {
	case "GetProtocolInfo":
		response = connectionResponse(action, `
<u:GetProtocolInfoResponse xmlns:u="urn:schemas-upnp-org:service:ConnectionManager:1">
  <Source>
    http-get:*:audio/mpeg:*,
    http-get:*:audio/opus:*,
    http-get:*:audio/wav:*,
    http-get:*:audio/flac:*,
    http-get:*:audio/ogg:*,
    http-get:*:audio/aac:*
  </Source>
  <Sink></Sink>
</u:GetProtocolInfoResponse>`)

	case "GetCurrentConnectionIDs":
		response = connectionResponse(action, `
<u:GetCurrentConnectionIDsResponse xmlns:u="urn:schemas-upnp-org:service:ConnectionManager:1">
  <ConnectionIDs>0</ConnectionIDs>
</u:GetCurrentConnectionIDsResponse>`)

	case "GetCurrentConnectionInfo":
		response = connectionResponse(action, `
<u:GetCurrentConnectionInfoResponse xmlns:u="urn:schemas-upnp-org:service:ConnectionManager:1">
  <RcsID>-1</RcsID>
  <AVTransportID>-1</AVTransportID>
  <ProtocolInfo></ProtocolInfo>
  <PeerConnectionManager></PeerConnectionManager>
  <PeerConnectionID>-1</PeerConnectionID>
  <Direction>Output</Direction>
  <Status>OK</Status>
</u:GetCurrentConnectionInfoResponse>`)

	default:
		response = soapFaultResponse("401", "Invalid Action")
	}

	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.Write([]byte(response))
}

// ---- Service Descriptions ----

func (h *Handler) serveAVTransportDesc(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.Write([]byte(avTransportSCPD))
}

func (h *Handler) serveRenderingControlDesc(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.Write([]byte(renderingControlSCPD))
}

func (h *Handler) serveConnectionManagerDesc(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.Write([]byte(connectionManagerSCPD))
}

// ---- SOAP Helpers ----

func parseSOAPRequest(r *http.Request) ([]byte, error) {
	if r.Method != http.MethodPost {
		return nil, fmt.Errorf("method not POST")
	}

	buf := new(bytes.Buffer)
	buf.ReadFrom(r.Body)
	return buf.Bytes(), nil
}

func extractSOAPAction(body []byte) string {
	s := string(body)
	bodyIdx := strings.Index(s, "<s:Body")
	if bodyIdx < 0 {
		bodyIdx = strings.Index(s, "<Body")
		if bodyIdx < 0 {
			return ""
		}
	}
	gt := strings.Index(s[bodyIdx:], ">")
	if gt < 0 {
		return ""
	}
	afterBody := s[bodyIdx+gt+1:]

	start := strings.Index(afterBody, "<")
	if start < 0 {
		return ""
	}
	tagPart := afterBody[start+1:]

	tagPart = strings.TrimPrefix(tagPart, "u:")

	end := strings.IndexAny(tagPart, " >/\r\n")
	if end < 0 {
		return ""
	}
	return tagPart[:end]
}

func extractSOAPField(body []byte, field string) string {
	s := string(body)
	tag := "<" + field + ">"
	start := strings.Index(s, tag)
	if start < 0 {
		return ""
	}
	start += len(tag)
	end := strings.Index(s[start:], "</"+field+">")
	if end < 0 {
		return ""
	}
	return s[start : start+end]
}

func avTransportResponse(action, innerXML string) string {
	return soapResponse(action, innerXML)
}

func renderingResponse(action, innerXML string) string {
	return soapResponse(action, innerXML)
}

func connectionResponse(action, innerXML string) string {
	return soapResponse(action, innerXML)
}

func soapResponse(_, innerXML string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
  <s:Body>
%s
  </s:Body>
</s:Envelope>`, innerXML)
}

func soapFaultResponse(errorCode, errorDescription string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
  <s:Body>
    <s:Fault>
      <faultcode>s:Client</faultcode>
      <faultstring>UPnPError</faultstring>
      <detail>
        <UPnPError xmlns="urn:schemas-upnp-org:control-1-0">
          <errorCode>%s</errorCode>
          <errorDescription>%s</errorDescription>
        </UPnPError>
      </detail>
    </s:Fault>
  </s:Body>
</s:Envelope>`, errorCode, errorDescription)
}

func safeURL(raw string) string {
	if i := strings.Index(raw, "://"); i > 0 {
		if j := strings.Index(raw[i+3:], "@"); j > 0 {
			return raw[:i+3] + "***@" + raw[i+3+j+1:]
		}
	}
	return raw
}

func escapeXML(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	return s
}
