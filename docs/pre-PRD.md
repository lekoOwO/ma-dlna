# DLNA → Music Assistant Bridge Pre-PRD

## 1. Project Summary

Build a lightweight LAN service that exposes itself as a virtual DLNA/UPnP MediaRenderer, accepts playback requests from DLNA-capable controllers, and bridges those requests into Music Assistant (MA) / Home Assistant (HA) by serving a stable HTTP live stream generated from the original source URL.

The bridge should appear on the local network as a DLNA playback target, for example:

> Music Assistant Whole Home

When a user selects this DLNA renderer from a controller app, the bridge receives the media URI via UPnP AVTransport, starts an ffmpeg-based live ingest/transcode pipeline, exposes a local HTTP stream endpoint, and instructs MA to play that endpoint on a configured MA player or group.

## 2. Problem Statement

Many applications can play media to DLNA/UPnP renderers, but Music Assistant does not natively expose a DLNA renderer endpoint that arbitrary DLNA controllers can target.

A naive bridge could simply forward the DLNA-provided media URI to MA, but this is often unreliable because:

* The original media URL may only be reachable from the bridge host, not from MA or speakers.
* The URL may be a temporary URL served by the controller device.
* The source may require headers, redirects, or reconnection behavior.
* Some speakers or MA providers may not support the original codec/container.
* Multiple downstream players may attempt to fetch the same URL independently.
* Some sources do not support multiple clients, Range requests, or stable long-lived access.

Therefore, the bridge should act as a controlled media ingest and re-publishing layer rather than a pure URI forwarder.

## 3. Goals

### Primary Goals

1. Expose a virtual DLNA/UPnP MediaRenderer on the local network.
2. Accept basic DLNA playback commands:

   * `SetAVTransportURI`
   * `Play`
   * `Stop`
   * `Pause` if practical
3. In direct MA mode, pass the DLNA-provided source URI to MA; in HA-service mode, ingest it using ffmpeg.
4. Publish a stable local HTTP live stream URL from the bridge only for HA-service mode.
5. Call Music Assistant directly with the controller source URL, or Home Assistant as a legacy compatibility path with the bridge stream URL, on a configured MA player or group.
6. Support common audio sources with minimal configuration.
7. Work well in Docker / HA add-on style deployment with host networking.

### Secondary Goals

1. Support multiple downstream HTTP clients for the same playback session.
2. Provide a small ring buffer / prebuffer to improve playback stability.
3. Preserve basic metadata where available:

   * title
   * artist
   * album
   * album art URL if feasible
4. Provide simple observability:

   * current session
   * ffmpeg status
   * connected clients
   * bytes streamed
   * recent errors
5. Allow a future direct-proxy backend for cases where transcoding is undesirable.

## 4. Non-Goals

The project should not attempt to be a full DLNA server or full DLNA renderer implementation.

Out of scope for MVP:

* Video playback.
* Image playback.
* Full DLNA compliance certification.
* Gapless playback.
* Accurate seeking.
* Accurate duration reporting.
* Full playlist queue management.
* DRM circumvention.
* Spotify Connect / AirPlay / Chromecast receiver functionality.
* Perfect multi-room synchronization. This remains MA’s responsibility.
* Acting as a universal system-audio capture sink.
* Full media library indexing.

## 5. Target Users

Primary users are Home Assistant / Music Assistant users who:

* Have speakers already integrated into MA.
* Want DLNA-capable apps to be able to play to their MA speakers or MA groups.
* Are comfortable running a Docker container or HA add-on on the same LAN as HA/MA.
* Prefer a simple DLNA target over configuring app-specific integrations.

## 6. High-Level Architecture

```text
DLNA Controller / App
        ↓
Virtual DLNA MediaRenderer
        ↓
DLNA-MA Bridge Session Manager
        ↓
ffmpeg ingest/transcode process
        ↓
Bridge HTTP live stream endpoint for HA-service mode,
or original source URL for direct MA mode
        ↓
Music Assistant direct API / Home Assistant play_media call
        ↓
Configured MA player or MA group
        ↓
Speakers
```

The bridge consists of five major modules:

```text
dlna-ma-bridge
├── upnp/
│   ├── SSDP advertisement
│   ├── device description XML
│   ├── AVTransport service
│   ├── RenderingControl service
│   └── ConnectionManager service
├── sessions/
│   ├── source URI
│   ├── metadata
│   ├── playback state
│   ├── generated stream token
│   └── ffmpeg process handle
├── stream/
│   ├── HTTP live endpoint
│   ├── ring buffer
│   ├── prebuffer
│   └── multi-client fanout
├── ma_adapter/
│   ├── HA service call adapter
│   └── optional direct MA API adapter
└── config/
    ├── target entity/player
    ├── output format
    ├── network settings
    └── security settings
```

## 7. Preferred MVP Behavior

### 7.1 Startup

On startup, the service:

1. Loads configuration.
2. Starts HTTP server.
3. Starts SSDP responder / advertiser.
4. Publishes a UPnP device description as a MediaRenderer.
5. Exposes minimal service control endpoints for AVTransport, RenderingControl, and ConnectionManager.

### 7.2 Playback Flow

```text
1. DLNA controller discovers bridge via SSDP.
2. User selects bridge as playback target.
3. Controller sends SetAVTransportURI(source_uri, metadata).
4. Bridge stores source_uri and metadata in a new or current session.
5. Controller sends Play.
6. Bridge generates local stream URL:
   http://<public_base_url>/live/<session_id>.mp3?token=<token>
7. Bridge calls HA/MA play_media for configured target player/group.
8. MA/player requests the local stream URL.
9. Bridge starts ffmpeg if not already running.
10. ffmpeg ingests source_uri and outputs normalized audio to stdout.
11. Bridge streams ffmpeg output to all connected HTTP clients.
12. On Stop, bridge tells MA to stop and terminates ffmpeg.
```

### 7.3 Lazy ffmpeg Start

Prefer lazy ffmpeg startup:

* `SetAVTransportURI` stores the source.
* `Play` calls MA with the bridge URL.
* ffmpeg starts when the first downstream client requests `/live/<session>`.

This avoids losing the beginning of the stream while MA or speakers are connecting.

### 7.4 Default ffmpeg Output

MVP should prioritize compatibility over perfect quality.

Default output:

```text
Format: MP3
Codec: libmp3lame
Bitrate: 192 kbps
Channels: stereo
Sample rate: 44.1 kHz
Container: raw MP3 stream
Content-Type: audio/mpeg
```

Example command shape:

```bash
ffmpeg \
  -hide_banner -loglevel warning \
  -reconnect 1 \
  -reconnect_streamed 1 \
  -reconnect_delay_max 5 \
  -i "$SOURCE_URL" \
  -vn \
  -ac 2 \
  -ar 44100 \
  -codec:a libmp3lame \
  -b:a 192k \
  -f mp3 \
  pipe:1
```

The implementation should allow overriding output format in configuration.

## 8. Functional Requirements

### 8.1 UPnP / DLNA Renderer Facade

The bridge should advertise itself as:

```text
urn:schemas-upnp-org:device:MediaRenderer:1
```

Required services for MVP:

```text
AVTransport:1
RenderingControl:1
ConnectionManager:1
```

Required AVTransport actions:

* `SetAVTransportURI`
* `Play`
* `Stop`
* `Pause`
* `GetTransportInfo`
* `GetPositionInfo`
* `GetMediaInfo`

Required RenderingControl actions:

* `GetVolume`
* `SetVolume`
* `GetMute`
* `SetMute`

Required ConnectionManager actions:

* `GetProtocolInfo`
* `GetCurrentConnectionIDs`
* `GetCurrentConnectionInfo`

Only a minimal, pragmatic implementation is required. Many methods may return static or approximate values as long as common controllers remain compatible.

### 8.2 Session Management

Each playback item should be represented as a session:

```json
{
  "session_id": "abc123",
  "source_uri": "http://source.local/audio.flac",
  "metadata_raw": "<DIDL-Lite>...</DIDL-Lite>",
  "metadata_parsed": {
    "title": "Track Title",
    "artist": "Artist",
    "album": "Album",
    "album_art_uri": "http://..."
  },
  "state": "stopped|playing|paused|error",
  "stream_url": "http://bridge.local:8787/live/abc123.mp3?token=...",
  "created_at": "...",
  "updated_at": "...",
  "ffmpeg_pid": 1234,
  "error": null
}
```

MVP may support only one active session at a time. The architecture should not preclude future queue support.

### 8.3 HTTP Live Stream Endpoint

Endpoint:

```text
GET /live/{session_id}.mp3?token={token}
HEAD /live/{session_id}.mp3?token={token}
```

Expected behavior:

* Validate session and token.
* On `HEAD`, return stable headers where possible.
* On `GET`, attach the client to the session stream.
* Start ffmpeg lazily if needed.
* Stream audio bytes to the client.
* Support multiple clients reading from the same live session.
* Close client connection cleanly when ffmpeg exits or session stops.

Recommended headers:

```http
Content-Type: audio/mpeg
Cache-Control: no-store
Connection: keep-alive
Accept-Ranges: none
```

Range support is not required for ffmpeg-live MVP. This is a live stream, not file playback.

### 8.4 Ring Buffer / Fanout

The stream module should support one ffmpeg process per active session and fan out its stdout to downstream clients.

Recommended MVP:

```yaml
stream:
  prebuffer_bytes: 32768        # 32 KiB
  ring_buffer_bytes: 8388608    # 8 MiB
  init_segment_bytes: 32768     # 32 KiB
  max_replay_bytes: 65536       # 64 KiB cap for init + prebuffer replay
  max_clients_per_session: 16
```

Behavior:

* ffmpeg stdout writes into a ring buffer.
* New clients start from either:

  * the current write position, or
  * a small prebuffer window if available.
* If a client is too slow and falls behind the ring buffer, disconnect it.
* If no clients remain for a grace period, terminate ffmpeg.

### 8.5 MA Adapter

The preferred control path is Music Assistant's direct HTTP API. The legacy Home Assistant service path remains available for deployments that cannot expose MA's API to the bridge.

Direct MA mode calls `/api` with commands such as `player_queues/play_media`, `player_queues/pause`, `player_queues/stop`, `player_queues/get_active_queue`, `player_queues/seek`, and `players/cmd/volume_set`. The bridge sends a metadata-bearing MA track object backed by the original controller source URL, so MA handles fetching, buffering, grouping, and synchronization without a bridge ffmpeg `/live` hop.

Direct MA config:

```yaml
ma_adapter:
  mode: "direct"

music_assistant:
  url: "http://music-assistant.local:8095"
  token: "${MA_TOKEN}"
  target_player_id: "whole_home"
```

The HA service path calls Home Assistant services through the HA REST API.

Required config:

```yaml
ha:
  url: "http://homeassistant.local:8123"
  token: "<long-lived-access-token>"
  target_entity_id: "media_player.whole_home"
```

Default play action should use one of these approaches, configurable:

1. `music_assistant.play_media` if available and configured.
2. `media_player.play_media` fallback.

Example conceptual payload for HA service call:

```json
{
  "entity_id": "media_player.whole_home",
  "media_content_id": "http://192.168.1.10:8787/live/abc123.mp3?token=...",
  "media_content_type": "music"
}
```

The exact service schema should be implementation-configurable because MA and HA versions may differ.

Recommended HA-service config shape:

```yaml
ma_adapter:
  mode: "ha_service"
  play_service: "music_assistant.play_media"
  stop_service: "media_player.media_stop"
  pause_service: "media_player.media_pause"
  volume_service: "media_player.volume_set"
```

### 8.6 Metadata Handling

MVP should store raw DIDL-Lite metadata from `SetAVTransportURI`.

Best effort parsing:

* title
* creator / artist
* album
* album art URI
* resource protocolInfo
* source MIME type if present

Metadata is used for logging, UPnP responses, source MIME detection, and direct MA API playback payloads. It is not required for HA-service playback success.

### 8.7 Volume Handling

RenderingControl volume actions should map to the configured MA/HA target if practical.

MVP behavior:

* Store an internal volume level.
* On `SetVolume`, call HA `media_player.volume_set` for the target entity.
* On `GetVolume`, return last known internal value.

If HA volume call fails, do not fail the whole session.

### 8.8 Pause Behavior

Pause records the elapsed playback position, calls MA pause, and terminates
ffmpeg. Resume/Play restarts ffmpeg with `-ss <position>` to seek to the
recorded position.

```text
Pause = record elapsed time + call MA pause + kill ffmpeg
Resume/Play = call MA play + restart ffmpeg with -ss <position>
```

If the source supports HTTP Range requests, ffmpeg will seek to the correct
position. If the source does not support seeking (e.g. live streams), ffmpeg
starts from the current stream position. The bridge does not explicitly probe
for Range support — it relies on ffmpeg's input seeking behavior.

## 9. Configuration

Example `config.yaml`:

```yaml
server:
  bind_host: "0.0.0.0"
  http_port: 8787
  public_base_url: "http://192.168.1.10:8787"

upnp:
  friendly_name: "Music Assistant Whole Home"
  manufacturer: "dlna-ma-bridge"
  model_name: "DLNA to Music Assistant Bridge"
  uuid: "auto"
  advertise_interval_seconds: 1800

ha:
  url: "http://homeassistant.local:8123"
  token: "${HA_TOKEN}"
  target_entity_id: "media_player.whole_home"

ma_adapter:
  mode: "ha_service"
  play_service: "music_assistant.play_media"
  fallback_play_service: "media_player.play_media"
  stop_service: "media_player.media_stop"
  pause_service: "media_player.media_pause"
  volume_service: "media_player.volume_set"

music_assistant:
  url: "http://music-assistant.local:8095"
  token: "${MA_TOKEN}"
  target_player_id: "whole_home"

ffmpeg:
  binary: "ffmpeg"
  output_format: "mp3"
  codec: "libmp3lame"
  bitrate: "192k"
  sample_rate: 44100
  channels: 2
  reconnect: true
  extra_input_args: []
  extra_output_args: []

stream:
  prebuffer_bytes: 32768
  ring_buffer_bytes: 8388608
  init_segment_bytes: 32768
  max_replay_bytes: 65536
  max_clients_per_session: 16
  no_client_grace_seconds: 10
  startup_timeout_seconds: 15

security:
  require_stream_token: true
  allowed_source_cidrs:
    - "192.168.0.0/16"
    - "10.0.0.0/8"
    - "172.16.0.0/12"
  blocked_source_cidrs:
    - "127.0.0.0/8"
    - "169.254.169.254/32"
  allow_public_sources: true

logging:
  level: "info"
```

## 10. Deployment Requirements

### 10.1 Docker

Recommended Docker Compose:

```yaml
services:
  dlna-ma-bridge:
    image: ghcr.io/example/dlna-ma-bridge:latest
    network_mode: host
    environment:
      HA_TOKEN: "..."
    volumes:
      - ./config.yaml:/config/config.yaml:ro
    restart: unless-stopped
```

Host networking is strongly recommended because UPnP/DLNA discovery depends on SSDP multicast.

### 10.2 HA Add-on Future

A future HA add-on should:

* Include ffmpeg.
* Use host networking if supported.
* Expose config through add-on options.
* Provide logs in HA add-on UI.
* Optionally discover HA URL and token through supervisor APIs if feasible.

## 11. Security Considerations

The bridge can become an HTTP fetcher/proxy for arbitrary URLs. Implement SSRF-style safeguards.

MVP safeguards:

1. Require random token on stream URLs.
2. Restrict source URLs by scheme:

   * allow `http`
   * allow `https`
   * optionally allow `file` only if explicitly enabled; disabled by default
3. Block sensitive link-local or loopback destinations unless explicitly allowed.
4. Limit ffmpeg process lifetime.
5. Limit concurrent sessions.
6. Limit maximum downstream clients.
7. Avoid logging full URLs if they may contain credentials or tokens.

Recommended blocked CIDRs by default:

```text
127.0.0.0/8
169.254.0.0/16
::1/128
fe80::/10
```

Need a config escape hatch because some legitimate DLNA controller URLs may be private LAN URLs.

## 12. State Machine

Session states:

```text
idle
  ↓ SetAVTransportURI
loaded
  ↓ Play
starting
  ↓ downstream GET + ffmpeg ready
playing
  ↓ Pause
paused
  ↓ Play
starting
  ↓ Stop
stopped
  ↓ error
error
```

Simplified transitions:

| Event                         | From                  | To       | Action                             |
| ----------------------------- | --------------------- | -------- | ---------------------------------- |
| SetAVTransportURI             | any                   | loaded   | Store source URI and metadata      |
| Play                          | loaded/paused/stopped | starting | Call MA play_media with bridge URL |
| First downstream GET          | starting              | playing  | Start ffmpeg and stream output     |
| Stop                          | any                   | stopped  | Call MA stop, kill ffmpeg          |
| Pause                         | playing               | paused   | Call MA pause, kill ffmpeg         |
| ffmpeg exits unexpectedly     | playing/starting      | error    | Disconnect clients, log error      |
| No clients after grace period | playing               | stopped  | Kill ffmpeg                        |

## 13. Error Handling

Important error cases:

### Invalid source URI

Behavior:

* Reject or mark session error.
* Return UPnP error if possible.
* Do not call MA.

### ffmpeg startup failure

Behavior:

* HTTP stream returns 502 or closes connection.
* Session state becomes `error`.
* Log stderr excerpt.

### HA/MA play_media failure

Behavior:

* Session state becomes `error`.
* Do not start ffmpeg unless a downstream client still requests the stream.
* Return useful log message.

### Downstream client disconnects

Behavior:

* Remove client.
* Continue ffmpeg while other clients remain.
* If no clients remain, start grace timer.

### Source stalls

Behavior:

* ffmpeg reconnect flags should handle common HTTP stream failures.
* If ffmpeg exits, mark session error.

## 14. Observability

Recommended endpoints:

```text
GET /healthz
GET /status
GET /sessions
GET /sessions/{session_id}
```

Example `/status`:

```json
{
  "status": "ok",
  "active_session_id": "abc123",
  "upnp_friendly_name": "Music Assistant Whole Home",
  "http_base_url": "http://192.168.1.10:8787",
  "sessions": 1,
  "clients": 2,
  "ffmpeg_running": true
}
```

Logs should include:

* SSDP startup
* source URI host only, not necessarily full URL
* session created
* play requested
* ffmpeg started/stopped
* downstream client connected/disconnected
* HA/MA service call success/failure

## 15. Suggested Technology Choices

Two viable implementation paths:

### Option A: Go

Pros:

* Single static-ish binary.
* Good fit for streaming HTTP servers.
* Easy process supervision for ffmpeg.
* Good Docker deployment.

Potential libraries:

* `net/http` for HTTP server.
* `os/exec` for ffmpeg.
* UPnP/SSDP via existing library or minimal custom implementation.

### Option B: Python

Pros:

* Closer to HA ecosystem.
* Easier XML/SOAP prototyping.
* Easier HA API calls.

Potential libraries:

* `aiohttp` for HTTP server.
* `asyncio.subprocess` for ffmpeg.
* `async-upnp-client` or custom UPnP service code.
* `defusedxml` / `lxml` for metadata parsing.

Recommendation: start in Go if the priority is robust streaming daemon behavior; start in Python if the priority is fastest UPnP/HA ecosystem prototyping.

## 16. MVP Acceptance Criteria

MVP is successful when:

1. A DLNA controller can discover the bridge as a renderer.
2. A controller can send a direct audio URL to the bridge.
3. Pressing play in the controller causes the configured MA player/group to start playback.
4. MA receives a bridge-hosted HTTP stream URL, not the original source URL.
5. The bridge starts one ffmpeg process for the active session.
6. At least one downstream client can stream MP3 audio from the bridge endpoint.
7. Stop from the DLNA controller stops MA playback and terminates ffmpeg.
8. The service runs in Docker with host networking.
9. Logs are sufficient to debug discovery, playback, ffmpeg, and HA call failures.

## 17. Test Plan

### 17.1 Manual Test Matrix

Test controllers:

* BubbleUPnP or equivalent Android DLNA controller.
* VLC local network playback target if applicable.
* Windows “Cast to Device” if applicable.
* foobar2000 UPnP plugin if available.

Test sources:

* Direct MP3 HTTP URL.
* Direct FLAC HTTP URL.
* DLNA server hosted file URL.
* Internet radio stream.
* HLS stream if ffmpeg handles it.

Test targets:

* MA group.
* Single MA player.
* Chromecast-backed MA player.
* AirPlay-backed MA player.
* Sonos-backed MA player.

### 17.2 Automated Tests

Suggested tests:

* Config loading.
* Session state transitions.
* Token validation.
* HA service payload generation.
* ffmpeg command generation.
* Ring buffer fanout behavior.
* Client disconnect handling.
* UPnP SOAP request parsing for key actions.

### 17.3 Integration Tests

Use a local HTTP fixture server serving:

* MP3 file.
* FLAC file.
* Slow streaming response.
* Redirected URL.
* Broken stream.

Validate that bridge exposes playable MP3 output via `/live/{session}`.

## 18. Future Enhancements

### 18.1 Direct Proxy Backend

Add a backend that proxies the original source without transcoding:

```yaml
streaming:
  backend: "direct_proxy"
```

Useful for:

* Preserving original audio quality.
* Lower CPU usage.
* Sources already compatible with target players.

Requires better handling of:

* Range requests.
* Content-Length.
* MIME passthrough.
* Playlist rewriting.

### 18.2 Hybrid Backend

Add automatic backend selection:

```text
if source is audio/mpeg and reachable: direct proxy
else: ffmpeg live transcode
```

### 18.3 Playlist and HLS Handling

For direct proxy mode:

* Parse M3U / PLS.
* Rewrite HLS playlists and segment URLs.
* Proxy segment requests.

For ffmpeg mode, ffmpeg may already handle many playlist types.

### 18.4 Queue Support

Support multiple `SetAVTransportURI` calls and map them into MA queue operations.

### 18.5 Better Metadata Display

Pass parsed metadata to MA if MA API supports it.

### 18.6 Web UI

Add a minimal web UI:

* Current session.
* Source info.
* ffmpeg logs.
* Connected clients.
* Manual stop/restart.

### 18.7 HA Add-on

Package as Home Assistant add-on with configuration UI.

## 19. Open Questions

1. Which implementation language should be used: Go or Python?
2. Does the target MA setup fetch media through MA server, or do individual players fetch the URL directly?
3. Should pause kill ffmpeg in MVP, or keep ingest running?
4. Should output default to MP3 192k, MP3 320k, AAC, or FLAC?
5. Should MVP support only one active session, or multiple virtual renderers / targets?
6. How much UPnP compliance is needed for target controller apps?
7. What controllers should be considered must-work for v0.1?

## 20. Recommended v0.1 Scope

Build the smallest reliable version:

* One virtual DLNA renderer.
* One configured MA target entity/group.
* One active session.
* ffmpeg live transcode to MP3.
* HTTP `/live/{session}.mp3` endpoint.
* HA REST service calls for play/stop/pause/volume.
* Static-ish UPnP responses sufficient for common controllers.
* Docker deployment with host networking.
* Basic logs and `/healthz`.

Do not implement direct proxy, Range support, HLS rewrite, multi-session queue, or web UI in v0.1 unless they are required to make the first target controller work.

## 21. Suggested Initial Milestones

### Milestone 1: Streaming Core

* Config loader.
* HTTP server.
* ffmpeg process wrapper.
* `/live/test.mp3?source=...` development endpoint.
* Ring buffer fanout.

### Milestone 2: HA / MA Adapter

* HA REST client.
* `play_media` call.
* `stop` call.
* Configurable target entity.

### Milestone 3: Minimal UPnP Renderer

* SSDP advertise.
* Device description XML.
* AVTransport `SetAVTransportURI`, `Play`, `Stop`.
* Validate with at least one DLNA controller.

### Milestone 4: Productization

* Dockerfile.
* Compose example.
* Logging cleanup.
* `/healthz` and `/status`.
* Documentation.

### Milestone 5: Compatibility Pass

* Test with multiple controllers and sources.
* Add missing static UPnP action responses.
* Add config knobs for ffmpeg args and content type.

## 22. One-Line Product Definition

A virtual DLNA MediaRenderer that turns DLNA playback requests into a stable ffmpeg-backed HTTP live stream and asks Music Assistant to play that stream on a configured speaker or group.
