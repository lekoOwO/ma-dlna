package upnp

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/leko/ma-dlna/internal/config"
	"github.com/leko/ma-dlna/internal/maadapter"
	"github.com/leko/ma-dlna/internal/session"
)

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

func (h *Handler) baseURL() string {
	return h.cfg.Server.PublicBaseURL
}

var ssdpAddr = &net.UDPAddr{IP: net.IPv4(239, 255, 255, 250), Port: 1900}

// multicastInterfaces returns all IPv4 addresses on multicast-capable UP interfaces.
func multicastInterfaces() []*net.UDPAddr {
	var out []*net.UDPAddr
	ifaces, err := net.Interfaces()
	if err != nil {
		return out
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagMulticast == 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipn, ok := a.(*net.IPNet)
			if !ok || ipn.IP.To4() == nil || ipn.IP.IsLoopback() {
				continue
			}
			out = append(out, &net.UDPAddr{IP: ipn.IP})
		}
	}
	return out
}

// M-SEARCH listener on all interfaces.
func (h *Handler) mserve(ctx context.Context) {
	conns := make([]*net.UDPConn, 0)
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()

	for _, laddr := range multicastInterfaces() {
		conn, err := net.ListenMulticastUDP("udp4", nil, &net.UDPAddr{IP: laddr.IP, Port: ssdpAddr.Port})
		if err != nil {
			slog.Warn("M-SEARCH listen failed on interface", "ip", laddr.IP, "error", err)
			continue
		}
		conns = append(conns, conn)
	}

	if len(conns) == 0 {
		slog.Warn("No multicast interfaces available for M-SEARCH")
		return
	}
	slog.Info("M-SEARCH listening", "interfaces", len(conns))

	// Unified receive loop across all connections.
	buf := make([]byte, 4096)
	type msg struct {
		data       []byte
		remoteAddr *net.UDPAddr
		conn       *net.UDPConn
	}
	ch := make(chan msg, 8)

	for _, conn := range conns {
		go func(c *net.UDPConn) {
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

	for {
		select {
		case <-ctx.Done():
			return
		case m := <-ch:
			body := string(m.data)
			if !strings.Contains(body, "M-SEARCH") {
				continue
			}
			if !strings.Contains(body, "urn:schemas-upnp-org:device:MediaRenderer:1") {
				continue
			}
			slog.Debug("M-SEARCH received", "from", m.remoteAddr.String())
			resp := h.mserveResponse()
			m.conn.WriteToUDP([]byte(resp), m.remoteAddr)
			slog.Debug("M-SEARCH response sent", "to", m.remoteAddr.String())
		}
	}
}

func (h *Handler) mserveResponse() string {
	base := h.baseURL()
	return fmt.Sprintf(
		"HTTP/1.1 200 OK\r\n"+
			"CACHE-CONTROL: max-age=%d\r\n"+
			"EXT:\r\n"+
			"LOCATION: %s/device.xml\r\n"+
			"SERVER: Linux/6.8 UPnP/1.0 dlna-ma-bridge/0.1\r\n"+
			"ST: urn:schemas-upnp-org:device:MediaRenderer:1\r\n"+
			"USN: %s::urn:schemas-upnp-org:device:MediaRenderer:1\r\n"+
			"\r\n",
		h.cfg.UPnP.AdvertiseIntervalSecs,
		base,
		h.deviceUUID,
	)
}

// SSDP Advertisement
func (h *Handler) ssdpLoop(ctx context.Context) {
	interval := time.Duration(h.cfg.UPnP.AdvertiseIntervalSecs) * time.Second
	if interval <= 0 {
		interval = 30 * time.Minute
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	h.broadcastSSDP(h.ssdpAliveMessage())

	for {
		select {
		case <-ctx.Done():
			h.broadcastSSDP(h.ssdpByeByeMessage())
			return
		case <-ticker.C:
			h.broadcastSSDP(h.ssdpAliveMessage())
		}
	}
}

func (h *Handler) broadcastSSDP(msg string) {
	for _, laddr := range multicastInterfaces() {
		conn, err := net.DialUDP("udp4", laddr, ssdpAddr)
		if err != nil {
			slog.Debug("SSDP dial failed", "iface", laddr.IP, "error", err)
			continue
		}
		conn.Write([]byte(msg))
		conn.Close()
	}
	slog.Debug("SSDP broadcast done")
}

func (h *Handler) ssdpAliveMessage() string {
	base := h.baseURL()
	return fmt.Sprintf(
		"NOTIFY * HTTP/1.1\r\n"+
			"HOST: 239.255.255.250:1900\r\n"+
			"CACHE-CONTROL: max-age=%d\r\n"+
			"LOCATION: %s/device.xml\r\n"+
			"NT: %s\r\n"+
			"NTS: ssdp:alive\r\n"+
			"SERVER: %s/%s UPnP/1.0 dlna-ma-bridge/0.1\r\n"+
			"USN: %s::urn:schemas-upnp-org:device:MediaRenderer:1\r\n"+
			"\r\n",
		h.cfg.UPnP.AdvertiseIntervalSecs,
		base,
		"urn:schemas-upnp-org:device:MediaRenderer:1",
		"Linux", "6.8",
		h.deviceUUID,
	)
}

func (h *Handler) ssdpByeByeMessage() string {
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

// Device Description XML
func (h *Handler) serveDeviceDesc(w http.ResponseWriter, r *http.Request) {
	base := h.baseURL()
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

// AVTransport Service
func (h *Handler) serveAVTransport(w http.ResponseWriter, r *http.Request) {
	body, err := parseSOAPRequest(r)
	if err != nil {
		http.Error(w, "Bad request", 400)
		return
	}

	action := extractSOAPAction(body)

	slog.Info("AVTransport action", "action", action)

	var response string

	switch action {
	case "SetAVTransportURI":
		instanceID := extractSOAPField(body, "InstanceID")
		uri := extractSOAPField(body, "CurrentURI")
		metadata := extractSOAPField(body, "CurrentURIMetaData")

		h.sessionMgr.Create(uri, metadata)
		response = avTransportResponse(action, fmt.Sprintf(`
<u:SetAVTransportURIResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"/>`))
		_ = instanceID

	case "Play":
		instanceID := extractSOAPField(body, "InstanceID")
		active := h.sessionMgr.ActiveSession()
		if active != nil {
			h.sessionMgr.Play(active.ID)
			h.maAdapter.PlayMedia(
				h.cfg.HA.TargetEntityID,
				active.StreamURL,
				"music",
			)
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

// RenderingControl Service
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

// ConnectionManager Service
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
    http-get:*:audio/aac:*,
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

// Service Description XMLs
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

// SOAP Helpers
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

	// Find first element tag after <s:Body>
	start := strings.Index(afterBody, "<")
	if start < 0 {
		return ""
	}
	tagPart := afterBody[start+1:] // skip '<'

	// Check for namespace prefix
	tagPart = strings.TrimPrefix(tagPart, "u:")

	// Find end of tag name (space, >, or />)
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
	return soapResponse("AVTransport", action, innerXML)
}

func renderingResponse(action, innerXML string) string {
	return soapResponse("RenderingControl", action, innerXML)
}

func connectionResponse(action, innerXML string) string {
	return soapResponse("ConnectionManager", action, innerXML)
}

func soapResponse(_, _ string, innerXML string) string {
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

func escapeXML(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	return s
}
