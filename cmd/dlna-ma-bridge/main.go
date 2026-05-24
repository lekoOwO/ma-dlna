package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/leko/ma-dlna/internal/config"
	"github.com/leko/ma-dlna/internal/maadapter"
	"github.com/leko/ma-dlna/internal/session"
	"github.com/leko/ma-dlna/internal/stream"
	"github.com/leko/ma-dlna/internal/upnp"
	"github.com/leko/ma-dlna/internal/version"
)

func main() {
	configPath := flag.String("config", "config.yaml", "Path to configuration file")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("Failed to load configuration", "error", err)
		os.Exit(1)
	}

	setupLogging(cfg)

	slog.Info("Starting dlna-ma-bridge", "version", version.Version, "commit", version.Commit)
	slog.Debug("Config", "ha_url", cfg.HA.URL, "target", cfg.HA.TargetEntityID,
		"output", cfg.FFmpeg.OutputFormat, "codec", cfg.FFmpeg.Codec, "bitrate", cfg.FFmpeg.Bitrate,
		"auto_base_url", cfg.UPnP.AutoBaseURL, "public_base_url", cfg.Server.PublicBaseURL)

	streamer := stream.NewStreamer(cfg)
	sessionMgr := session.NewManager(cfg, streamer)
	streamer.SetTokenValidator(sessionMgr.ValidateToken)
	maAdapter := maadapter.New(cfg)
	upnpHandler := upnp.NewHandler(cfg, sessionMgr, maAdapter)

	mux := http.NewServeMux()
	mux.HandleFunc("/live/", streamer.ServeHTTP)
	mux.HandleFunc("/healthz", healthzHandler)
	mux.HandleFunc("/status", statusHandler(cfg, sessionMgr, streamer))
	mux.HandleFunc("/sessions", sessionsHandler(sessionMgr))
	mux.HandleFunc("/sessions/", sessionByIDHandler(sessionMgr))
	upnpHandler.RegisterUPnPEndpoints(mux)

	wrapped := httpLogMiddleware(mux)

	httpServer := &http.Server{
		Addr:         net.JoinHostPort(cfg.Server.BindHost, fmt.Sprintf("%d", cfg.Server.HTTPPort)),
		Handler:      wrapped,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0,
		IdleTimeout:  60 * time.Second,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := upnpHandler.Start(ctx); err != nil {
		slog.Error("Failed to start UPnP handler", "error", err)
		os.Exit(1)
	}

	go func() {
		slog.Info("HTTP server listening", "addr", httpServer.Addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("HTTP server error", "error", err)
			os.Exit(1)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	slog.Info("Shutting down", "signal", sig.String())

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	upnpHandler.Stop()
	sessionMgr.Shutdown()
	httpServer.Shutdown(shutdownCtx)
	slog.Info("Shutdown complete")
}

func setupLogging(cfg *config.Config) {
	level := slog.LevelInfo
	switch cfg.Logging.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	slog.SetLogLoggerLevel(level)
}

func healthzHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func statusHandler(cfg *config.Config, mgr *session.Manager, streamer *stream.Streamer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		active := mgr.ActiveSession()
		resp := map[string]any{
			"status":            "ok",
			"upnp_friendly_name": cfg.UPnP.FriendlyName,
			"http_base_url":     cfg.Server.PublicBaseURL,
			"sessions":          mgr.Count(),
			"clients":           streamer.TotalClients(),
		}
		if active != nil {
			resp["active_session_id"] = active.ID
			resp["ffmpeg_running"] = streamer.IsRunning(active.ID)
		}
		writeJSON(w, http.StatusOK, resp)
	}
}

func sessionsHandler(mgr *session.Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, mgr.AllSessions())
	}
}

func sessionByIDHandler(mgr *session.Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Path[len("/sessions/"):]
		s := mgr.Get(id)
		if s == nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
			return
		}
		writeJSON(w, http.StatusOK, s)
	}
}

func httpLogMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slog.Debug("HTTP request", "method", r.Method, "path", r.URL.Path, "remote", r.RemoteAddr)
		next.ServeHTTP(w, r)
	})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
