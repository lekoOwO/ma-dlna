# DLNA → Music Assistant Bridge Pre-PRD

## 1. Project Summary

Build a lightweight LAN service that exposes itself as a virtual DLNA/UPnP MediaRenderer and forwards controller playback requests to a configured Music Assistant player or group.

The bridge appears on the local network as a DLNA playback target, for example:

> Music Assistant Whole Home

When a controller selects this renderer, the bridge receives the media URI via UPnP AVTransport, extracts DIDL-Lite metadata where available, and sends the original source URL plus metadata to Music Assistant through the Music Assistant WebSocket API. Music Assistant is responsible for fetching the source, buffering, grouping, sync, and speaker playback.

## 2. Goals

### Primary Goals

1. Expose a virtual DLNA/UPnP MediaRenderer on the local network.
2. Accept common DLNA AVTransport commands:
   * `SetAVTransportURI`
   * `Play`
   * `Stop`
   * `Pause`
   * `Seek` with `REL_TIME`
   * `GetTransportInfo`
   * `GetPositionInfo`
   * `GetMediaInfo`
3. Control only Music Assistant directly; there is no secondary service backend.
4. Use Music Assistant's WebSocket API at `/ws` for commands and state reads.
5. Send Music Assistant the original DLNA controller source URL and parsed metadata, not a transcoded bridge stream.
6. Keep DLNA transport state and position aligned with the configured Music Assistant player or group.
7. Work well in Docker with host networking on the same LAN as controllers and Music Assistant.

### Secondary Goals

1. Preserve useful metadata where controllers provide it:
   * title
   * artist / creator
   * album
   * album art URL
   * duration
   * content type from DIDL `protocolInfo`
2. Provide simple observability:
   * active session
   * parsed metadata
   * current state
   * recent errors
3. Keep UPnP behavior conservative and predictable for consumer controller apps.

## 3. Non-Goals

The project does not attempt to be a full DLNA server or certified DLNA renderer.

Out of scope:

* Video playback.
* Image playback.
* Full DLNA compliance certification.
* Gapless playback guarantees.
* DRM circumvention.
* Spotify Connect / AirPlay / Chromecast receiver functionality.
* Multi-room synchronization logic. This remains Music Assistant's responsibility.
* Media library indexing.

## 4. Target Users

Primary users are Music Assistant users who:

* Have speakers already integrated into Music Assistant.
* Want DLNA-capable apps to play to Music Assistant speakers or groups.
* Are comfortable running a Docker container on the same LAN as Music Assistant.
* Prefer a simple DLNA target over app-specific integrations.

## 5. High-Level Architecture

```text
DLNA Controller / App
        ↓
Virtual DLNA MediaRenderer
        ↓
DLNA-MA Session Manager
        ↓
Music Assistant WebSocket API
        ↓
Configured Music Assistant player or group
        ↓
Speakers
```

Major modules:

```text
dlna-ma-bridge
├── upnp/
│   ├── SSDP advertisement
│   ├── device description XML
│   ├── AVTransport service
│   ├── RenderingControl service
│   └── ConnectionManager service
├── session/
│   ├── source URI
│   ├── raw and parsed metadata
│   ├── current DLNA transport state
│   └── generated compatibility stream URL/token
├── maadapter/
│   ├── Music Assistant WebSocket handshake/auth
│   ├── command request/response handling
│   ├── queue resolution
│   └── playback state/position reads
└── config/
    ├── YAML config loading
    ├── Music Assistant target validation
    └── source URL security policy
```

## 6. Music Assistant Integration

The bridge connects to:

```text
ws://<music-assistant-host>:8095/ws
wss://<music-assistant-host>/ws when music_assistant.url uses https/wss
```

After connecting, the bridge authenticates with:

```json
{
  "message_id": "auth-1",
  "command": "auth",
  "args": { "token": "<MA token>" }
}
```

Commands use the same `message_id`, `command`, and `args` shape as Music Assistant's documented WebSocket API.

Required commands:

* `player_queues/get_active_queue`
* `player_queues/get`
* `player_queues/play_media`
* `player_queues/play`
* `player_queues/pause`
* `player_queues/stop`
* `player_queues/seek`
* `players/cmd/volume_set`

Playback uses `player_queues/play_media` with `option: "replace"`. The `media` payload is a metadata-bearing track object whose `item_id` and provider mapping URL point to the original controller source URL.

## 7. DLNA State and Position Sync

Music Assistant is the source of truth for the selected player or group.

The bridge maps Music Assistant playback state to UPnP AVTransport state:

| Music Assistant | UPnP AVTransport |
| --- | --- |
| `playing` | `PLAYING` |
| `paused` | `PAUSED_PLAYBACK` |
| `idle` | `STOPPED` after debounce / end detection |
| `unknown` | preserve current state unless command fails |

`GetPositionInfo` returns Music Assistant queue/player position when available. While Music Assistant reports `playing`, the bridge adjusts `elapsed_time` using `elapsed_time_last_updated` to avoid stale progress.

`GetTransportInfo` opportunistically syncs current session state from Music Assistant so external pause/resume actions on the selected player are reflected in DLNA controller UI.

## 8. Configuration

Minimal configuration:

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
  auto_base_url: true

music_assistant:
  url: "http://musicassistant.local:8095"
  token: "${MA_TOKEN}"
  target_player_id: "whole_home"

security:
  allowed_source_cidrs:
    - "192.168.0.0/16"
    - "10.0.0.0/8"
    - "172.16.0.0/12"
  blocked_source_cidrs:
    - "169.254.0.0/16"
  allow_public_sources: true
  allow_loopback_sources: false

logging:
  level: "info"
```

`music_assistant.token` should be a Music Assistant access token. `target_player_id` may be a player or group queue target managed by Music Assistant.

## 9. Security Model

The UPnP control plane is LAN-trusted. Any controller that can reach the bridge can send playback commands.

Source URL policy should reject unsafe local targets by default and allow only explicitly configured CIDR ranges plus public sources when enabled. The bridge may expose source URLs and DIDL metadata in UPnP responses to LAN controllers, matching normal DLNA renderer assumptions.

## 10. Test Plan

Required automated coverage:

* Config validation requires Music Assistant URL, token, and target player ID.
* Music Assistant adapter uses WebSocket `/ws`, authenticates first, sends command `message_id`, and handles command errors.
* `PlayMedia` sends the original source URL and metadata to Music Assistant.
* UPnP `Play` does not start ffmpeg or a bridge stream.
* UPnP pause/resume/stop/seek call the configured Music Assistant target.
* `GetPositionInfo` uses Music Assistant position.
* `GetTransportInfo` reflects external Music Assistant pause state.
* XML escaping and UPnP service descriptions remain well-formed.
