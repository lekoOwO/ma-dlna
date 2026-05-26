package upnp

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	neturl "net/url"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/leko/ma-dlna/internal/config"
	"github.com/leko/ma-dlna/internal/maadapter"
	"github.com/leko/ma-dlna/internal/session"
	"github.com/leko/ma-dlna/internal/version"
)

func serverString() string {
	return runtime.GOOS + "/ UPnP/1.0 dlna-ma-bridge/" + version.Version
}

func localAddrFromConn(r *http.Request) net.IP {
	a := r.Context().Value(http.LocalAddrContextKey)
	if a == nil {
		return nil
	}
	switch addr := a.(type) {
	case *net.TCPAddr:
		return addr.IP
	case net.TCPAddr:
		return addr.IP
	}
	return nil
}

func uuidUSN(id string) string {
	for strings.HasPrefix(id, "uuid:") {
		id = id[5:]
	}
	return "uuid:" + id
}

type eventSubscriber struct {
	sid        string
	callback   string
	callbackIP string
	service    string
	seq        int
	expiresAt  time.Time
}

type Handler struct {
	cfg               *config.Config
	sessionMgr        *session.Manager
	maAdapter         *maadapter.Adapter
	mu                sync.RWMutex
	rcMu              sync.Mutex
	playbackMonMu     sync.Mutex
	playbackMonCancel context.CancelFunc
	volume            int
	prevVolume        int
	muted             bool
	ssdpCancel        context.CancelFunc
	deviceUUID        string
	subscribers       map[string]*eventSubscriber
}

func NewHandler(cfg *config.Config, sessionMgr *session.Manager, maAdapter *maadapter.Adapter) *Handler {
	return &Handler{
		cfg:         cfg,
		sessionMgr:  sessionMgr,
		maAdapter:   maAdapter,
		volume:      50,
		prevVolume:  50,
		deviceUUID:  cfg.UPnP.UUID,
		subscribers: make(map[string]*eventSubscriber),
	}
}

func (h *Handler) Start(ctx context.Context) error {
	ctx, h.ssdpCancel = context.WithCancel(ctx)
	go h.ssdpLoop(ctx)
	go h.mserve(ctx)
	slog.Info("UPnP handler started", "friendly_name", escapeXML(h.cfg.UPnP.FriendlyName), "uuid", h.deviceUUID)
	return nil
}

func (h *Handler) Stop() {
	if h.ssdpCancel != nil {
		h.ssdpCancel()
	}
	h.stopPlaybackMonitor()
	slog.Info("UPnP handler stopped")
}

// notifyCurrentSession sends an AVTransport event only if the given sessionID
// is still current. Synchronous so request-handler event ordering is preserved
// (notifySubscribers itself uses internal goroutines for HTTP delivery).
func (h *Handler) notifyCurrentSession(sessionID, lastChangeXML string) {
	cur := h.sessionMgr.CurrentSession()
	if cur == nil || cur.ID != sessionID {
		return
	}
	h.notifySubscribers("AVTransport", lastChangeXML)
}

// NotifyError sends AVTransport LastChange on stream/session errors (ffmpeg crash,
// first-client timeout, pipe failure) so subscribers see ERROR_OCCURRED without polling.
func (h *Handler) NotifyError(sessionID string) {
	h.notifyCurrentSession(sessionID, avTransportLastChangeStatus("STOPPED", "ERROR_OCCURRED"))
}

// NotifyPlaying sends AVTransport LastChange when the first /live client connects,
// meaning HA/MA has actually started consuming the stream. It also starts the
// playback monitor to detect when HA/MA reports playback ended.
func (h *Handler) NotifyPlaying(sessionID string, genID uint64) {
	h.notifyCurrentSession(sessionID, avTransportLastChange("PLAYING"))
	h.startPlaybackMonitor(sessionID, genID)
}

// startPlaybackMonitor polls HA/MA media_player state periodically and fires
// STOPPED when the entity reports playback has ended (state != "playing").
// This is the long-term replacement for relying on ffmpeg EOF as playback-ended.
func (h *Handler) startPlaybackMonitor(sessionID string, genID uint64) {
	h.playbackMonMu.Lock()
	defer h.playbackMonMu.Unlock()

	if h.playbackMonCancel != nil {
		h.playbackMonCancel()
		h.playbackMonCancel = nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	h.playbackMonCancel = cancel

	go func() {
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()

		wasPlaying := false
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				state, err := h.maAdapter.GetEntityState(h.cfg.HA.TargetEntityID)
				if err != nil {
					slog.Debug("Playback monitor poll failed", "entity", h.cfg.HA.TargetEntityID, "error", err)
					continue
				}
				switch state {
				case "playing":
					wasPlaying = true
				case "idle", "off", "standby":
					if wasPlaying {
						slog.Info("Playback monitor detected playback ended", "entity", h.cfg.HA.TargetEntityID, "ha_state", state, "session_id", sessionID)
						h.sessionMgr.MarkStoppedIfGeneration(sessionID, genID)
						h.notifyCurrentSession(sessionID, avTransportLastChange("STOPPED"))
						return
					}
				case "paused":
					if wasPlaying {
						h.notifyCurrentSession(sessionID, avTransportLastChange("PAUSED_PLAYBACK"))
					}
				}
			}
		}
	}()
}

func (h *Handler) stopPlaybackMonitor() {
	h.playbackMonMu.Lock()
	defer h.playbackMonMu.Unlock()
	if h.playbackMonCancel != nil {
		h.playbackMonCancel()
		h.playbackMonCancel = nil
	}
}

// NotifyDeliveryEnded is a no-op hook for ffmpeg EOF.
// Playback-ended (STOPPED) is driven by the HA/MA playback monitor, not by
// stream EOF, because HA/MA may still be playing buffered audio after
// ffmpeg finishes delivering the stream.
func (h *Handler) NotifyDeliveryEnded(sessionID string) {}

func (h *Handler) activeSession() *session.Session {
	return h.sessionMgr.CurrentSession()
}

func (h *Handler) RegisterUPnPEndpoints(mux *http.ServeMux) {
	mux.HandleFunc("/device.xml", h.serveDeviceDesc)
	mux.HandleFunc("/avtransport/control", h.serveAVTransport)
	mux.HandleFunc("/avtransport/event", h.serveEvent)
	mux.HandleFunc("/rendering/control", h.serveRenderingControl)
	mux.HandleFunc("/rendering/event", h.serveEvent)
	mux.HandleFunc("/connection/control", h.serveConnectionManager)
	mux.HandleFunc("/connection/event", h.serveEvent)
	mux.HandleFunc("/service/AVTransport/desc.xml", h.serveAVTransportDesc)
	mux.HandleFunc("/service/RenderingControl/desc.xml", h.serveRenderingControlDesc)
	mux.HandleFunc("/service/ConnectionManager/desc.xml", h.serveConnectionManagerDesc)
}

// ---- Base URL helpers ----

func (h *Handler) baseURLForRequest(r *http.Request) string {
	if h.cfg.UPnP.AutoBaseURL && r.Host != "" {
		localIP := localAddrFromConn(r)
		if localIP != nil && !localIP.IsLoopback() && !localIP.IsUnspecified() &&
			!localIP.IsLinkLocalUnicast() && !localIP.IsLinkLocalMulticast() {
			return "http://" + net.JoinHostPort(localIP.String(), fmt.Sprintf("%d", h.cfg.Server.HTTPPort))
		}
		host, port, err := net.SplitHostPort(r.Host)
		if err != nil {
			host = r.Host
			port = fmt.Sprintf("%d", h.cfg.Server.HTTPPort)
		}
		ip := net.ParseIP(host)
		if ip != nil && !ip.IsLoopback() && !ip.IsUnspecified() && matchingIP(ip) != nil {
			return "http://" + net.JoinHostPort(host, port)
		}
	}
	// All paths fall through to the common fallback helper
	return h.fallbackBaseURL(r)
}

// fallbackBaseURL returns the configured PublicBaseURL, avoiding 0.0.0.0 if
// possible by matching the requester's IP against local interfaces.
func (h *Handler) fallbackBaseURL(r *http.Request) string {
	if strings.Contains(h.cfg.Server.PublicBaseURL, "0.0.0.0") {
		remoteIP, _, _ := net.SplitHostPort(r.RemoteAddr)
		if lip := matchingIP(net.ParseIP(remoteIP)); lip != nil {
			return "http://" + net.JoinHostPort(lip.String(), fmt.Sprintf("%d", h.cfg.Server.HTTPPort))
		}
	}
	return h.cfg.Server.PublicBaseURL
}

func (h *Handler) baseURLForIP(ip net.IP) string {
	if h.cfg.UPnP.AutoBaseURL && ip != nil {
		return "http://" + net.JoinHostPort(ip.String(), fmt.Sprintf("%d", h.cfg.Server.HTTPPort))
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
			if !h.matchesSearchTarget(body) {
				continue
			}
			key := msearchKey{ip: m.remoteAddr.IP.String(), st: st}
			if _, exists := history[key]; exists {
				continue
			}
			history[key] = time.Now()

			localIP := matchingIP(m.remoteAddr.IP)
			base := h.baseURLForIP(localIP)
			slog.Debug("M-SEARCH responded", "from", m.remoteAddr.String(), "st", st, "local_ip", localIP)
			if st == "ssdp:all" {
				for _, t := range ssdpAllTargets(h.deviceUUID) {
					m.conn.WriteToUDP([]byte(h.mserveResponse(base, t)), m.remoteAddr)
				}
			} else {
				m.conn.WriteToUDP([]byte(h.mserveResponse(base, st)), m.remoteAddr)
			}
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

func (h *Handler) matchesSearchTarget(body string) bool {
	for _, st := range []string{
		"urn:schemas-upnp-org:device:MediaRenderer:1",
		"ssdp:all",
		"upnp:rootdevice",
		"urn:schemas-upnp-org:service:AVTransport:1",
		"urn:schemas-upnp-org:service:RenderingControl:1",
		"urn:schemas-upnp-org:service:ConnectionManager:1",
		uuidUSN(h.deviceUUID),
	} {
		if strings.Contains(body, st) {
			return true
		}
	}
	return false
}

func ssdpAllTargets(deviceUUID string) []string {
	return []string{
		"upnp:rootdevice",
		uuidUSN(deviceUUID),
		"urn:schemas-upnp-org:device:MediaRenderer:1",
		"urn:schemas-upnp-org:service:AVTransport:1",
		"urn:schemas-upnp-org:service:RenderingControl:1",
		"urn:schemas-upnp-org:service:ConnectionManager:1",
	}
}

func (h *Handler) mserveResponse(base, st string) string {
	u := uuidUSN(h.deviceUUID)
	usn := u + "::" + st
	if strings.HasPrefix(st, "uuid:") {
		usn = u
	}
	return fmt.Sprintf(
		"HTTP/1.1 200 OK\r\n"+
			"CACHE-CONTROL: max-age=%d\r\n"+
			"EXT:\r\n"+
			"LOCATION: %s/device.xml\r\n"+
			"SERVER: %s\r\n"+
			"ST: %s\r\n"+
			"USN: %s\r\n"+
			"\r\n",
		h.cfg.UPnP.AdvertiseIntervalSecs,
		base,
		serverString(),
		st,
		usn,
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

func (h *Handler) broadcastSSDP(msgFn func(string, string) string) {
	ifaces := multicastInterfaces()
	slog.Debug("SSDP broadcast", "interfaces", len(ifaces))
	// UPnP spec requires alive for rootdevice, uuid, device type, and each service
	targets := []string{
		"upnp:rootdevice",
		uuidUSN(h.deviceUUID),
		"urn:schemas-upnp-org:device:MediaRenderer:1",
		"urn:schemas-upnp-org:service:AVTransport:1",
		"urn:schemas-upnp-org:service:RenderingControl:1",
		"urn:schemas-upnp-org:service:ConnectionManager:1",
	}
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
		base := h.baseURLForIP(ip)
		for _, nt := range targets {
			conn.Write([]byte(msgFn(base, nt)))
		}
		conn.Close()
	}
}

func (h *Handler) ssdpAliveMsg(base, nt string) string {
	u := uuidUSN(h.deviceUUID)
	usn := u + "::" + nt
	if strings.HasPrefix(nt, "uuid:") {
		usn = u
	}
	return fmt.Sprintf(
		"NOTIFY * HTTP/1.1\r\n"+
			"HOST: 239.255.255.250:1900\r\n"+
			"CACHE-CONTROL: max-age=%d\r\n"+
			"LOCATION: %s/device.xml\r\n"+
			"NT: %s\r\n"+
			"NTS: ssdp:alive\r\n"+
			"SERVER: %s\r\n"+
			"USN: %s\r\n"+
			"\r\n",
		h.cfg.UPnP.AdvertiseIntervalSecs,
		base,
		nt,
		serverString(),
		usn,
	)
}

func (h *Handler) ssdpByeByeMsg(_ string, nt string) string {
	u := uuidUSN(h.deviceUUID)
	usn := u + "::" + nt
	if strings.HasPrefix(nt, "uuid:") {
		usn = u
	}
	return fmt.Sprintf(
		"NOTIFY * HTTP/1.1\r\n"+
			"HOST: 239.255.255.250:1900\r\n"+
			"NT: %s\r\n"+
			"NTS: ssdp:byebye\r\n"+
			"USN: %s\r\n"+
			"\r\n",
		nt,
		usn,
	)
}

// ---- Device Description XML ----

func (h *Handler) serveDeviceDesc(w http.ResponseWriter, r *http.Request) {
	base := h.baseURLForRequest(r)
	slog.Debug("Device description served", "remote", r.RemoteAddr, "base", base)
	xml := fmt.Sprintf(`<?xml version="1.0"?>
<root xmlns="urn:schemas-upnp-org:device-1-0"
      xmlns:dlna="urn:schemas-dlna-org:device-1-0">
  <specVersion>
    <major>1</major>
    <minor>0</minor>
  </specVersion>
  <device>
    <deviceType>urn:schemas-upnp-org:device:MediaRenderer:1</deviceType>
    <friendlyName>%s</friendlyName>
    <manufacturer>%s</manufacturer>
    <manufacturerURL>https://github.com/lekoOwO/ma-dlna</manufacturerURL>
    <modelDescription>DLNA to Music Assistant Bridge</modelDescription>
    <modelName>%s</modelName>
    <modelNumber>%s</modelNumber>
    <UDN>%s</UDN>
    <dlna:X_DLNADOC xmlns:dlna="urn:schemas-dlna-org:device-1-0">DMR-1.50</dlna:X_DLNADOC>
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
		escapeXML(h.cfg.UPnP.FriendlyName),
		escapeXML(h.cfg.UPnP.Manufacturer),
		escapeXML(h.cfg.UPnP.ModelName),
		version.Version,
		h.deviceUUID,
		base, base, base,
		base, base, base,
		base, base, base,
	)

	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.Write([]byte(xml))
}

// ---- Event Subscription ----

func (h *Handler) serveEvent(w http.ResponseWriter, r *http.Request) {
	cb := r.Header.Get("CALLBACK")
	svc := eventServiceFromPath(r.URL.Path)
	slog.Debug("Event subscription", "method", r.Method, "path", r.URL.Path,
		"remote", r.RemoteAddr, "sid", r.Header.Get("SID"), "callback", safeURL(cb))

	switch r.Method {
	case "SUBSCRIBE":
		sid := r.Header.Get("SID")
		if sid != "" {
			h.mu.Lock()
			sub, ok := h.subscribers[sid]
			if ok {
				sub.expiresAt = time.Now().Add(1800 * time.Second)
			}
			h.mu.Unlock()
			if !ok {
				http.Error(w, "unknown SID", http.StatusPreconditionFailed)
				return
			}
			w.Header().Set("SID", sid)
			w.Header().Set("TIMEOUT", "Second-1800")
			w.WriteHeader(http.StatusOK)
			return
		}
		// New subscription
		sid = "uuid:" + generateSubscriptionUUID()
		if cb == "" {
			http.Error(w, "missing CALLBACK", http.StatusBadRequest)
			return
		}
		pinIP, err := h.validateCallback(cb, r.RemoteAddr)
		if err != nil {
			slog.Warn("Event callback rejected", "callback", safeURL(cb), "error", err)
			http.Error(w, "invalid CALLBACK", http.StatusPreconditionFailed)
			return
		}
		w.Header().Set("SID", sid)
		w.Header().Set("TIMEOUT", "Second-1800")
		w.Header().Set("SERVER", serverString())
		w.WriteHeader(http.StatusOK)

		if h.subscribers != nil {
			h.mu.Lock()
			h.subscribers[sid] = &eventSubscriber{sid: sid, callback: cb, callbackIP: pinIP, service: svc, expiresAt: time.Now().Add(1800 * time.Second)}
			h.mu.Unlock()
		}
		go h.sendInitialEvent(cb, pinIP, sid, svc)

	case "UNSUBSCRIBE":
		sid := r.Header.Get("SID")
		if h.subscribers != nil {
			h.mu.Lock()
			delete(h.subscribers, sid)
			h.mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

func eventServiceFromPath(path string) string {
	switch {
	case strings.Contains(path, "/rendering/"):
		return "RenderingControl"
	case strings.Contains(path, "/connection/"):
		return "ConnectionManager"
	default:
		return "AVTransport"
	}
}

func (h *Handler) validateCallback(callback string, remoteAddr string) (string, error) {
	remoteIP, _, _ := net.SplitHostPort(remoteAddr)
	urls := extractCallbackURLs(callback)
	var pinIP string
	if len(urls) == 0 {
		return "", fmt.Errorf("no valid callback URL")
	}
	for _, rawURL := range urls {
		u, err := neturl.Parse(rawURL)
		if err != nil {
			return "", fmt.Errorf("invalid callback URL %q: %w", rawURL, err)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return "", fmt.Errorf("callback scheme not allowed: %s", u.Scheme)
		}
		host := u.Hostname()
		ips, err := net.LookupIP(host)
		if err != nil {
			return "", fmt.Errorf("cannot resolve callback host: %w", err)
		}
		// Pin to the requester IP if we can verify it, otherwise use first resolved IP.
		if remoteIP != "" {
			matched := false
			for _, ip := range ips {
				if ip.String() == remoteIP {
					matched = true
					if pinIP == "" {
						pinIP = ip.String()
					}
					break
				}
			}
			if !matched {
				return "", fmt.Errorf("callback host does not match requester IP")
			}
		} else if pinIP == "" {
			pinIP = ips[0].String()
		}
		for _, ip := range ips {
			if ip.IsLoopback() && !h.cfg.Security.AllowLoopbackSources {
				return "", fmt.Errorf("callback IP blocked: %s (loopback)", ip)
			}
			if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
				ip.IsMulticast() || ip.IsUnspecified() {
				return "", fmt.Errorf("callback IP blocked: %s", ip)
			}
		}
	}
	return pinIP, nil
}

func (h *Handler) sendInitialEvent(callback, pinIP, sid, service string) {
	urls := extractCallbackURLs(callback)
	if len(urls) == 0 {
		return
	}

	body := h.initialEventBody(service)

	for _, u := range urls {
		req, err := http.NewRequest("NOTIFY", u, strings.NewReader(body))
		if err != nil {
			slog.Debug("Event NOTIFY create failed", "url", safeURL(u), "error", err)
			continue
		}
		parsed, _ := neturl.Parse(u)
		if parsed != nil {
			req.Host = parsed.Host
		}
		req.Header.Set("Content-Type", "text/xml; charset=utf-8")
		req.Header.Set("NT", "upnp:event")
		req.Header.Set("NTS", "upnp:propchange")
		req.Header.Set("SID", sid)
		req.Header.Set("SEQ", "0")
		req.Header.Set("SERVER", serverString())

		transport := &http.Transport{
			DisableKeepAlives: true,
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				if pinIP != "" {
					_, port, _ := net.SplitHostPort(addr)
					addr = net.JoinHostPort(pinIP, port)
				}
				d := &net.Dialer{Timeout: 5 * time.Second}
				return d.DialContext(ctx, network, addr)
			},
		}
		client := &http.Client{
			Transport: transport,
			Timeout:   5 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
		resp, err := client.Do(req)
		if err != nil {
			slog.Warn("Event NOTIFY network error", "url", safeURL(u), "error", err)
			continue
		}
		resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			slog.Debug("Initial event sent", "url", safeURL(u), "sid", sid, "status", resp.StatusCode)
		} else {
			slog.Warn("Event NOTIFY rejected", "url", safeURL(u), "sid", sid, "status", resp.StatusCode)
		}
	}
}

func (h *Handler) initialEventBody(service string) string {
	switch service {
	case "RenderingControl":
		h.mu.RLock()
		vol := h.volume
		muted := h.muted
		h.mu.RUnlock()
		l := renderingControlLastChange(vol, muted)
		return fmt.Sprintf(`<?xml version="1.0"?>
	<e:propertyset xmlns:e="urn:schemas-upnp-org:event-1-0">
	  <e:property>
	    <LastChange>%s</LastChange>
	  </e:property>
	</e:propertyset>`, l)
	case "ConnectionManager":
		return `<?xml version="1.0"?>
	<e:propertyset xmlns:e="urn:schemas-upnp-org:event-1-0">
	  <e:property>
	    <LastChange>&lt;Event xmlns=&quot;urn:schemas-upnp-org:metadata-1-0/CM/&quot;&gt;&lt;InstanceID val=&quot;0&quot;&gt;&lt;SinkProtocolInfo val=&quot;http-get:*:audio/mpeg:*,http-get:*:audio/ogg:*,http-get:*:audio/wav:*,http-get:*:audio/flac:*,http-get:*:audio/aac:*&quot;/&gt;&lt;SourceProtocolInfo val=&quot;&quot;/&gt;&lt;/InstanceID&gt;&lt;/Event&gt;</LastChange>
	  </e:property>
	</e:propertyset>`
	default:
		var s *session.Session
		if h.sessionMgr != nil {
			s = h.sessionMgr.CurrentSession()
		}
		state := "STOPPED"
		status := "OK"
		if s != nil {
			switch s.State {
			case session.StatePlaying:
				state = "PLAYING"
			case session.StatePaused:
				state = "PAUSED_PLAYBACK"
			case session.StateStarting:
				state = "TRANSITIONING"
			case session.StateLoaded:
				state = "STOPPED"
			case session.StateError:
				state = "STOPPED"
				status = "ERROR_OCCURRED"
			}
		}
		l := avTransportLastChangeStatus(state, status)
		return fmt.Sprintf(`<?xml version="1.0"?>
	<e:propertyset xmlns:e="urn:schemas-upnp-org:event-1-0">
	  <e:property>
	    <LastChange>%s</LastChange>
	  </e:property>
	</e:propertyset>`, l)
	}
}

func (h *Handler) notifySubscribers(service, lastChangeXML string) {
	if h.subscribers == nil {
		return
	}
	h.mu.Lock()
	subs := make([]subSnapshot, 0)
	now := time.Now()
	for _, sub := range h.subscribers {
		if now.After(sub.expiresAt) {
			delete(h.subscribers, sub.sid)
			continue
		}
		if sub.service == service {
			sub.seq++
			subs = append(subs, subSnapshot{
				sid:        sub.sid,
				callback:   sub.callback,
				callbackIP: sub.callbackIP,
				service:    sub.service,
				seq:        sub.seq,
			})
		}
	}
	h.mu.Unlock()

	for _, s := range subs {
		go h.sendStateChange(s, lastChangeXML)
	}
}

type subSnapshot struct {
	sid        string
	callback   string
	callbackIP string
	service    string
	seq        int
}

func (h *Handler) sendStateChange(snap subSnapshot, lastChangeXML string) {
	body := fmt.Sprintf(`<?xml version="1.0"?>
<e:propertyset xmlns:e="urn:schemas-upnp-org:event-1-0">
  <e:property>
    <LastChange>%s</LastChange>
  </e:property>
</e:propertyset>`, escapeXML(lastChangeXML))

	for _, u := range extractCallbackURLs(snap.callback) {
		req, err := http.NewRequest("NOTIFY", u, strings.NewReader(body))
		if err != nil {
			slog.Debug("Event NOTIFY create failed", "url", safeURL(u), "error", err)
			continue
		}
		parsed, _ := neturl.Parse(u)
		if parsed != nil {
			req.Host = parsed.Host
		}
		req.Header.Set("Content-Type", "text/xml; charset=utf-8")
		req.Header.Set("NT", "upnp:event")
		req.Header.Set("NTS", "upnp:propchange")
		req.Header.Set("SID", snap.sid)
		req.Header.Set("SEQ", fmt.Sprintf("%d", snap.seq))
		req.Header.Set("SERVER", serverString())

		transport := &http.Transport{
			DisableKeepAlives: true,
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				if snap.callbackIP != "" {
					_, port, _ := net.SplitHostPort(addr)
					addr = net.JoinHostPort(snap.callbackIP, port)
				}
				d := &net.Dialer{Timeout: 5 * time.Second}
				return d.DialContext(ctx, network, addr)
			},
		}
		client := &http.Client{
			Transport: transport,
			Timeout:   5 * time.Second,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
		resp, err := client.Do(req)
		if err != nil {
			slog.Warn("State change NOTIFY error", "url", safeURL(u), "error", err)
			continue
		}
		resp.Body.Close()
		slog.Debug("State change NOTIFY sent", "service", snap.service, "url", safeURL(u), "seq", snap.seq)
	}
}

func avTransportLastChange(state string) string {
	return avTransportLastChangeStatus(state, "OK")
}

func avTransportLastChangeStatus(state, status string) string {
	return fmt.Sprintf(`<Event xmlns="urn:schemas-upnp-org:metadata-1-0/AVT/"><InstanceID val="0"><TransportState val="%s"/><TransportStatus val="%s"/><CurrentPlayMode val="NORMAL"/><TransportPlaySpeed val="1"/></InstanceID></Event>`, state, status)
}

func renderingControlLastChange(vol int, muted bool) string {
	muteVal := "0"
	if muted {
		muteVal = "1"
	}
	return fmt.Sprintf(`<Event xmlns="urn:schemas-upnp-org:metadata-1-0/RCS/"><InstanceID val="0"><Volume val="%d" channel="Master"/><Mute val="%s" channel="Master"/></InstanceID></Event>`, vol, muteVal)
}

func extractCallbackURLs(callback string) []string {
	// CALLBACK format: <http://host:port/path> or multiple
	var urls []string
	for _, part := range strings.Split(callback, ">") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, "<") {
			urls = append(urls, strings.TrimPrefix(part, "<"))
		}
	}
	return urls
}

func generateSubscriptionUUID() string {
	b := make([]byte, 16)
	_, _ = crand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// ---- AVTransport ----
//
// HA command failure semantics: local stream cleanup is applied before HA command.
// If HA fails, the session is marked as ERROR_OCCURRED and a SOAP fault is returned.
// This is not transactional rollback — local state changes (stop/pause) persist.

func (h *Handler) serveAVTransport(w http.ResponseWriter, r *http.Request) {
	body, err := parseSOAPRequest(r)
	if err != nil {
		http.Error(w, "Bad request", 400)
		return
	}

	action := extractActionName(r, body)

	slog.Info("AVTransport action", "action", action)

	var response string

	switch action {
	case "SetAVTransportURI":
		instanceID := extractSOAPField(body, "InstanceID")
		uri := extractSOAPField(body, "CurrentURI")
		metadata := extractSOAPField(body, "CurrentURIMetaData")

		slog.Info("SetAVTransportURI", "uri", safeURL(uri), "instance_id", instanceID)
		if err := h.cfg.Security.ValidateOrReject(uri); err != nil {
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(soapFaultResponse("714", "Illegal MIME-Type or source URI rejected")))
			return
		}
		streamBase := h.baseURLForRequest(r)
		if h.cfg.Server.StreamPublicBaseURL != "" {
			streamBase = h.cfg.Server.StreamPublicBaseURL
		}
		h.stopPlaybackMonitor()
		s := h.sessionMgr.CreateWithBase(uri, metadata, streamBase)
		h.notifyCurrentSession(s.ID, avTransportLastChange("STOPPED"))
		response = avTransportResponse(action, fmt.Sprintf(`
<u:SetAVTransportURIResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"/>`))
		_ = instanceID

	case "Play":
		instanceID := extractSOAPField(body, "InstanceID")
		_ = instanceID
		active := h.activeSession()
		if active == nil {
			slog.Warn("Play with no active session")
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(soapFaultResponse("701", "Transition not available")))
			return
		}
		slog.Info("Play requested, calling MA", "entity", h.cfg.HA.TargetEntityID, "stream_url", safeURL(active.StreamURL))
		if active.State == session.StatePlaying {
			response = avTransportResponse(action, fmt.Sprintf(`
	<u:PlayResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"/>`))
			break
		}
		h.sessionMgr.Play(active.ID)
		h.sessionMgr.StartStream(active.ID, active.SourceURI)
		if err := h.maAdapter.PlayMedia(
			h.cfg.HA.TargetEntityID,
			active.StreamURL,
			contentTypeForUPnP(h.cfg.FFmpeg.OutputFormat),
		); err != nil {
			slog.Error("PlayMedia failed, stopping stream", "session_id", active.ID, "error", err)
			h.sessionMgr.Stop(active.ID)
			h.sessionMgr.SetError(active.ID, err.Error())
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(soapFaultResponse("501", "Action Failed")))
			return
		}
		h.notifyCurrentSession(active.ID, avTransportLastChange("PLAYING"))
		response = avTransportResponse(action, fmt.Sprintf(`
<u:PlayResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"/>`))

	case "Stop":
		// Idempotent: returns success even without current media
		instanceID := extractSOAPField(body, "InstanceID")
		active := h.activeSession()
		if active != nil {
			h.sessionMgr.Stop(active.ID)
			if err := h.maAdapter.Stop(h.cfg.HA.TargetEntityID); err != nil {
				slog.Error("HA Stop failed", "session_id", active.ID, "error", err)
				h.sessionMgr.SetError(active.ID, err.Error())
				w.Header().Set("Content-Type", "text/xml; charset=utf-8")
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(soapFaultResponse("501", "Action Failed")))
				return
			}
			h.notifyCurrentSession(active.ID, avTransportLastChange("STOPPED"))
		}
		response = avTransportResponse(action, fmt.Sprintf(`
<u:StopResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"/>`))
		_ = instanceID

	case "Pause":
		instanceID := extractSOAPField(body, "InstanceID")
		active := h.activeSession()
		if active == nil {
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(soapFaultResponse("701", "Transition not available")))
			return
		}
		if active.State != session.StatePlaying && active.State != session.StateStarting {
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(soapFaultResponse("701", "Transition not available")))
			return
		}
		h.sessionMgr.Pause(active.ID)
		if err := h.maAdapter.Pause(h.cfg.HA.TargetEntityID); err != nil {
			slog.Error("HA Pause failed", "session_id", active.ID, "error", err)
			h.sessionMgr.Stop(active.ID)
			h.sessionMgr.SetError(active.ID, err.Error())
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(soapFaultResponse("501", "Action Failed")))
			return
		}
		h.notifyCurrentSession(active.ID, avTransportLastChange("PAUSED_PLAYBACK"))
		response = avTransportResponse(action, fmt.Sprintf(`
<u:PauseResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"/>`))
		_ = instanceID

	case "GetTransportInfo":
		state := "STOPPED"
		status := "OK"
		active := h.activeSession()
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
			case session.StateError:
				state = "STOPPED"
				status = "ERROR_OCCURRED"
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
		relTime := "00:00:00"
		dur := "00:00:00"
		uri := ""
		metadata := ""
		if h.sessionMgr != nil {
			if active := h.activeSession(); active != nil {
				elapsed := h.sessionMgr.Elapsed(active.ID)
				relTime = formatDurationUPnP(elapsed)
				uri = escapeXML(active.SourceURI)
				metadata = escapeXML(active.MetadataRaw)
				if active.Metadata != nil && active.Metadata.Duration != "" {
					dur = active.Metadata.Duration
				}
			}
		}
		response = avTransportResponse(action, fmt.Sprintf(`
	<u:GetPositionInfoResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1">
	  <Track>1</Track>
	  <TrackDuration>%s</TrackDuration>
	  <TrackMetaData>%s</TrackMetaData>
	  <TrackURI>%s</TrackURI>
	  <RelTime>%s</RelTime>
	  <AbsTime>00:00:00</AbsTime>
	  <RelCount>2147483647</RelCount>
	  <AbsCount>2147483647</AbsCount>
	</u:GetPositionInfoResponse>`, dur, metadata, uri, relTime))
	case "GetMediaInfo":
		dur := "00:00:00"
		uri := ""
		metadata := ""
		if s := h.activeSession(); s != nil {
			uri = escapeXML(s.SourceURI)
			metadata = escapeXML(s.MetadataRaw)
			if s.Metadata != nil && s.Metadata.Duration != "" {
				dur = s.Metadata.Duration
			}
		}
		response = avTransportResponse(action, fmt.Sprintf(`
	<u:GetMediaInfoResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1">
	  <NrTracks>1</NrTracks>
	  <MediaDuration>%s</MediaDuration>
	  <CurrentURI>%s</CurrentURI>
	  <CurrentURIMetaData>%s</CurrentURIMetaData>
	  <NextURI></NextURI>
	  <NextURIMetaData></NextURIMetaData>
	  <PlayMedium>NETWORK</PlayMedium>
	  <RecordMedium>NOT_IMPLEMENTED</RecordMedium>
	  <WriteStatus>NOT_IMPLEMENTED</WriteStatus>
	</u:GetMediaInfoResponse>`, dur, uri, metadata))

	case "Seek":
		instanceID := extractSOAPField(body, "InstanceID")
		unit := extractSOAPField(body, "Unit")
		target := extractSOAPField(body, "Target")
		_ = instanceID
		if unit != "REL_TIME" {
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(soapFaultResponse("710", "Seek mode not supported")))
			return
		}
		offset, err := parseRelTime(target)
		if err != nil {
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(soapFaultResponse("711", "Illegal seek target")))
			return
		}
		active := h.activeSession()
		if active == nil {
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(soapFaultResponse("701", "Transition not available")))
			return
		}
		switch active.State {
		case session.StatePlaying, session.StateStarting, session.StatePaused:
		default:
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(soapFaultResponse("701", "Transition not available")))
			return
		}
		slog.Info("Seek requested", "session_id", active.ID, "to", offset.Round(time.Second))
		wasPaused := active.State == session.StatePaused
		h.sessionMgr.Seek(active.ID, offset)
		if !wasPaused {
			h.sessionMgr.Resume(active.ID)
			go func() {
				if err := h.maAdapter.PlayMedia(
					h.cfg.HA.TargetEntityID,
					active.StreamURL,
					contentTypeForUPnP(h.cfg.FFmpeg.OutputFormat),
				); err != nil {
					slog.Error("Seek PlayMedia failed", "session_id", active.ID, "error", err)
					h.sessionMgr.Stop(active.ID)
					h.sessionMgr.SetError(active.ID, err.Error())
					return
				}
				h.notifyCurrentSession(active.ID, avTransportLastChange("PLAYING"))
			}()
		}
		response = avTransportResponse(action, fmt.Sprintf(`
	<u:SeekResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"/>`))

	case "Next":
		response = avTransportResponse(action, fmt.Sprintf(`
<u:NextResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"/>`))

	case "Previous":
		response = avTransportResponse(action, fmt.Sprintf(`
<u:PreviousResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"/>`))

	case "SetNextAVTransportURI":
		instanceID := extractSOAPField(body, "InstanceID")
		nextURI := extractSOAPField(body, "NextURI")
		metadata := extractSOAPField(body, "NextURIMetaData")
		_ = instanceID
		if h.sessionMgr != nil {
			if active := h.activeSession(); active != nil {
				h.sessionMgr.SetNextURI(active.ID, nextURI)
				slog.Debug("SetNextAVTransportURI", "session_id", active.ID, "next_uri", safeURL(nextURI))
			}
		}
		_ = metadata
		response = avTransportResponse(action, fmt.Sprintf(`
<u:SetNextAVTransportURIResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"/>`))

	case "SetPlayMode":
		newMode := extractSOAPField(body, "NewPlayMode")
		if h.sessionMgr != nil {
			if active := h.activeSession(); active != nil {
				h.sessionMgr.SetPlayMode(active.ID, newMode)
			}
		}
		slog.Debug("SetPlayMode", "mode", newMode)
		response = avTransportResponse(action, fmt.Sprintf(`
<u:SetPlayModeResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1"/>`))

	case "GetCurrentTransportActions":
		var actions string
		if h.sessionMgr != nil {
			if active := h.activeSession(); active != nil {
				actions = transportActionsForState(active.State)
			}
		}
		response = avTransportResponse(action, fmt.Sprintf(`
<u:GetCurrentTransportActionsResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1">
  <Actions>%s</Actions>
</u:GetCurrentTransportActionsResponse>`, actions))

	case "GetDeviceCapabilities":
		response = avTransportResponse(action, `
<u:GetDeviceCapabilitiesResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1">
  <PlayMedia>NETWORK</PlayMedia>
  <RecMedia>NOT_IMPLEMENTED</RecMedia>
  <RecQualityModes>NOT_IMPLEMENTED</RecQualityModes>
</u:GetDeviceCapabilitiesResponse>`)

	case "GetTransportSettings":
		response = avTransportResponse(action, `
<u:GetTransportSettingsResponse xmlns:u="urn:schemas-upnp-org:service:AVTransport:1">
  <PlayMode>NORMAL</PlayMode>
  <RecQualityMode>NOT_IMPLEMENTED</RecQualityMode>
</u:GetTransportSettingsResponse>`)

	default:
		slog.Warn("Unknown AVTransport action", "action", action)
		w.Header().Set("Content-Type", "text/xml; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(soapFaultResponse("401", "Invalid Action")))
		return
	}

	w.Header().Set("Content-Type", "text/xml; charset=utf-8")
	w.Write([]byte(response))
}

// ---- RenderingControl ----
//
// All RCS commands are synchronous against HA to keep renderer state consistent.
// This means SetVolume/SetMute may block up to the HA HTTP timeout (15s) on HA
// failure. The tradeoff is correctness over latency.

func (h *Handler) serveRenderingControl(w http.ResponseWriter, r *http.Request) {
	body, err := parseSOAPRequest(r)
	if err != nil {
		http.Error(w, "Bad request", 400)
		return
	}

	action := extractActionName(r, body)
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
		if vol < 0 {
			vol = 0
		} else if vol > 100 {
			vol = 100
		}

		h.rcMu.Lock()
		defer h.rcMu.Unlock()

		if err := h.maAdapter.SetVolume(h.cfg.HA.TargetEntityID, vol); err != nil {
			slog.Error("SetVolume failed", "error", err)
			w.Header().Set("Content-Type", "text/xml; charset=utf-8")
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(soapFaultResponse("501", "Action Failed")))
			return
		}
		h.mu.Lock()
		h.volume = vol
		if vol > 0 && h.muted {
			h.muted = false
		}
		muted := h.muted
		h.mu.Unlock()
		h.notifySubscribers("RenderingControl", renderingControlLastChange(vol, muted))

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
		targetMuted := muteStr == "1"

		h.rcMu.Lock()
		defer h.rcMu.Unlock()

		// Snapshot state under rcMu to serialize with SetVolume
		h.mu.RLock()
		curVol := h.volume
		curMuted := h.muted
		prevVol := h.prevVolume
		h.mu.RUnlock()
		// No-op but still sync HA to fix any drift
		if targetMuted == curMuted {
			syncVol := curVol
			if curMuted {
				syncVol = 0
			}
			if err := h.maAdapter.SetVolume(h.cfg.HA.TargetEntityID, syncVol); err != nil {
				slog.Error("SetMute drift sync failed", "error", err)
				w.Header().Set("Content-Type", "text/xml; charset=utf-8")
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(soapFaultResponse("501", "Action Failed")))
				return
			}
			response = renderingResponse(action, `
	<u:SetMuteResponse xmlns:u="urn:schemas-upnp-org:service:RenderingControl:1"/>`)
			break
		}

		if targetMuted {
			// Mute: save volume, then call HA
			if err := h.maAdapter.SetVolume(h.cfg.HA.TargetEntityID, 0); err != nil {
				slog.Error("SetMute failed", "error", err)
				w.Header().Set("Content-Type", "text/xml; charset=utf-8")
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(soapFaultResponse("501", "Action Failed")))
				return
			}
			h.mu.Lock()
			h.prevVolume = curVol
			h.muted = true
			h.mu.Unlock()
			h.notifySubscribers("RenderingControl", renderingControlLastChange(curVol, true))
		} else {
			// Unmute: restore volume
			restoreVol := curVol
			if prevVol > 0 {
				restoreVol = prevVol
			}
			if err := h.maAdapter.SetVolume(h.cfg.HA.TargetEntityID, restoreVol); err != nil {
				slog.Error("SetMute unmute failed", "error", err)
				w.Header().Set("Content-Type", "text/xml; charset=utf-8")
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte(soapFaultResponse("501", "Action Failed")))
				return
			}
			h.mu.Lock()
			h.volume = restoreVol
			h.muted = false
			h.mu.Unlock()
			h.notifySubscribers("RenderingControl", renderingControlLastChange(restoreVol, false))
		}

		response = renderingResponse(action, `
<u:SetMuteResponse xmlns:u="urn:schemas-upnp-org:service:RenderingControl:1"/>`)

	default:
		w.Header().Set("Content-Type", "text/xml; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(soapFaultResponse("401", "Invalid Action")))
		return
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

	action := extractActionName(r, body)
	slog.Debug("ConnectionManager action", "action", action)

	var response string

	switch action {
	case "GetProtocolInfo":
		base := "http-get:*:audio/mpeg:*,http-get:*:audio/ogg:*,http-get:*:audio/wav:*," +
			"http-get:*:audio/flac:*,http-get:*:audio/aac:*"
		if h.cfg.FFmpeg.OutputFormat != "opus" && h.cfg.FFmpeg.OutputFormat != "ogg" {
			base += ",http-get:*:audio/" + h.cfg.FFmpeg.OutputFormat + ":*"
		}
		sink := base
		response = connectionResponse(action, fmt.Sprintf(`
	<u:GetProtocolInfoResponse xmlns:u="urn:schemas-upnp-org:service:ConnectionManager:1">
	  <Sink>%s</Sink>
	  <Source></Source>
	</u:GetProtocolInfoResponse>`, sink))

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
  <Direction>Input</Direction>
  <Status>OK</Status>
</u:GetCurrentConnectionInfoResponse>`)

	default:
		w.Header().Set("Content-Type", "text/xml; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(soapFaultResponse("401", "Invalid Action")))
		return
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
	n, _ := buf.ReadFrom(io.LimitReader(r.Body, 4<<20+1))
	if n > 4<<20 {
		return nil, fmt.Errorf("request body too large")
	}
	return buf.Bytes(), nil
}

// extractActionName returns the SOAP action name from the body, falling back to
// the SOAPACTION header if the body parser returns empty (some controllers only
// set the header).
func extractActionName(r *http.Request, body []byte) string {
	if action := extractSOAPAction(body); action != "" {
		return action
	}
	// Fallback: parse from SOAPACTION header (format: "urn:...service:...#Action")
	// Header value may be quoted; strip quotes before parsing.
	if sa := r.Header.Get("SOAPACTION"); sa != "" {
		sa = strings.Trim(sa, `"`)
		if i := strings.LastIndexByte(sa, '#'); i >= 0 {
			return sa[i+1:]
		}
	}
	return ""
}

func extractSOAPAction(body []byte) string {
	decoder := xml.NewDecoder(bytes.NewReader(body))
	for {
		tok, err := decoder.Token()
		if err != nil {
			return ""
		}
		if se, ok := tok.(xml.StartElement); ok {
			// Find first element inside <s:Body> or <Body>
			if se.Name.Local == "Body" {
				// Next element is the action
				for {
					tok2, err2 := decoder.Token()
					if err2 != nil {
						return ""
					}
					if se2, ok2 := tok2.(xml.StartElement); ok2 {
						return se2.Name.Local
					}
				}
			}
		}
	}
}

func extractSOAPField(body []byte, field string) string {
	decoder := xml.NewDecoder(bytes.NewReader(body))
	for {
		tok, err := decoder.Token()
		if err != nil {
			return ""
		}
		if se, ok := tok.(xml.StartElement); ok {
			if se.Name.Local == field {
				var val string
				decoder.DecodeElement(&val, &se)
				return unescapeXML(val)
			}
		}
	}
}

func unescapeXML(s string) string {
	s = strings.ReplaceAll(s, "&lt;", "<")
	s = strings.ReplaceAll(s, "&gt;", ">")
	s = strings.ReplaceAll(s, "&quot;", "\"")
	s = strings.ReplaceAll(s, "&apos;", "'")
	s = strings.ReplaceAll(s, "&amp;", "&")
	return s
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

func transportActionsForState(s session.State) string {
	switch s {
	case session.StatePlaying:
		return "Play,Stop,Pause,Seek"
	case session.StatePaused:
		return "Play,Stop,Seek"
	case session.StateStarting:
		return "Stop"
	case session.StateLoaded:
		return "Play"
	default:
		return ""
	}
}

func parseRelTime(s string) (time.Duration, error) {
	var h, m, sec int
	_, err := fmt.Sscanf(s, "%d:%d:%d", &h, &m, &sec)
	if err != nil {
		return 0, err
	}
	return time.Duration(h)*time.Hour + time.Duration(m)*time.Minute + time.Duration(sec)*time.Second, nil
}

func contentTypeForUPnP(format string) string {
	switch format {
	case "mp3":
		return "audio/mpeg"
	case "opus":
		return "audio/ogg"
	case "ogg":
		return "audio/ogg"
	case "flac":
		return "audio/flac"
	case "aac":
		return "audio/aac"
	case "wav":
		return "audio/wav"
	default:
		return "audio/" + format
	}
}

func formatDurationUPnP(d time.Duration) string {
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}

func safeURL(raw string) string {
	// Strip userinfo (user:pass@)
	if i := strings.Index(raw, "://"); i > 0 {
		if j := strings.Index(raw[i+3:], "@"); j > 0 {
			raw = raw[:i+3] + "***@" + raw[i+3+j+1:]
		}
	}
	// Strip query string (contains token, signatures, etc.)
	if i := strings.IndexByte(raw, '?'); i >= 0 {
		raw = raw[:i] + "?..."
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
