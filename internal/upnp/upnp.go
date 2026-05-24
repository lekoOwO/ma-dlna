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

// SSDP Advertisement
func (h *Handler) ssdpLoop(ctx context.Context) {
	interval := time.Duration(h.cfg.UPnP.AdvertiseIntervalSecs) * time.Second
	if interval <= 0 {
		interval = 30 * time.Minute
	}

	addr := &net.UDPAddr{IP: net.IPv4(239, 255, 255, 250), Port: 1900}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	h.sendSSDP(addr)

	for {
		select {
		case <-ctx.Done():
			h.sendSSDPByeBye(addr)
			return
		case <-ticker.C:
			h.sendSSDP(addr)
		}
	}
}

func (h *Handler) sendSSDP(addr *net.UDPAddr) {
	conn, err := net.DialUDP("udp4", nil, addr)
	if err != nil {
		slog.Warn("Failed to send SSDP advertisement", "error", err)
		return
	}
	defer conn.Close()

	msg := h.ssdpAliveMessage()
	conn.Write([]byte(msg))
	slog.Debug("SSDP alive sent")
}

func (h *Handler) sendSSDPByeBye(addr *net.UDPAddr) {
	conn, err := net.DialUDP("udp4", nil, addr)
	if err != nil {
		return
	}
	defer conn.Close()

	msg := h.ssdpByeByeMessage()
	conn.Write([]byte(msg))
	slog.Debug("SSDP byebye sent")
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
		response = soapFaultResponse("UPnPError", "401", "Invalid Action")
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
		fmt.Sscanf(desired, "%d", &vol)

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
		response = soapFaultResponse("UPnPError", "401", "Invalid Action")
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
    http-get:*:audio/mp3:*,
    http-get:*:audio/wav:*,
    http-get:*:audio/flac:*,
    http-get:*:audio/x-flac:*,
    http-get:*:audio/ogg:*,
    http-get:*:audio/aac:*,
    http-get:*:audio/x-ms-wma:*,
    http-get:*:application/ogg:*
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
		response = soapFaultResponse("UPnPError", "401", "Invalid Action")
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
	start := strings.Index(s, "<u:")
	if start < 0 {
		start = strings.Index(s, "<")
		if start < 0 {
			return ""
		}
	}
	startTag := s[start:]
	end := strings.Index(startTag, " ")
	if end < 0 {
		end = strings.Index(startTag, ">")
	}
	if end < 0 {
		return ""
	}
	tag := startTag[:end]
	tag = strings.TrimPrefix(tag, "<u:")
	tag = strings.TrimPrefix(tag, "<")
	tag = strings.TrimSuffix(tag, "/")
	return tag
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

func soapResponse(service, action, innerXML string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="utf-8"?>
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
  <s:Body>
    <u:%sResponse xmlns:u="urn:schemas-upnp-org:service:%s:1">
%s
    </u:%sResponse>
  </s:Body>
</s:Envelope>`, action, service, innerXML, action)
}

func soapFaultResponse(faultCode, faultString, detail string) string {
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
</s:Envelope>`, faultString, detail)
}

func escapeXML(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, "\"", "&quot;")
	return s
}
