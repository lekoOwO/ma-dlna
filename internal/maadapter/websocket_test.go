package maadapter

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/leko/ma-dlna/internal/config"
)

func TestDirectAdapterUsesMusicAssistantWebSocket(t *testing.T) {
	type commandCall struct {
		Command string
		Args    map[string]any
	}
	calls := make(chan commandCall, 4)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ws" {
			t.Fatalf("adapter must use /ws WebSocket endpoint, got %s", r.URL.Path)
		}
		if r.Header.Get("Upgrade") == "" {
			t.Fatalf("adapter must open a WebSocket upgrade, got headers %v", r.Header)
		}
		conn, rw, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Fatalf("hijack websocket: %v", err)
		}
		defer conn.Close()

		accept := websocketAccept(r.Header.Get("Sec-WebSocket-Key"))
		_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\n")
		_, _ = rw.WriteString("Upgrade: websocket\r\n")
		_, _ = rw.WriteString("Connection: Upgrade\r\n")
		_, _ = rw.WriteString("Sec-WebSocket-Accept: " + accept + "\r\n\r\n")
		if err := rw.Flush(); err != nil {
			t.Fatalf("flush upgrade: %v", err)
		}

		writeServerText(t, rw, `{"server_id":"test","server_version":"2.8.8"}`)
		for {
			msg, err := readClientText(rw.Reader)
			if err != nil {
				return
			}
			var req struct {
				MessageID string         `json:"message_id"`
				Command   string         `json:"command"`
				Args      map[string]any `json:"args"`
			}
			if err := json.Unmarshal([]byte(msg), &req); err != nil {
				t.Fatalf("decode websocket request %q: %v", msg, err)
			}
			calls <- commandCall{Command: req.Command, Args: req.Args}

			switch req.Command {
			case "auth":
				if req.Args["token"] != "ma-token" {
					t.Fatalf("unexpected auth token: %v", req.Args["token"])
				}
				writeServerText(t, rw, `{"message_id":"`+req.MessageID+`","result":{"authenticated":true},"partial":false}`)
			case "player_queues/get_active_queue":
				if req.Args["player_id"] != "player123" {
					t.Fatalf("unexpected active queue player_id: %v", req.Args["player_id"])
				}
				writeServerText(t, rw, `{"message_id":"`+req.MessageID+`","result":{"queue_id":"queue123","state":"idle"},"partial":false}`)
			case "player_queues/play_media":
				writeServerText(t, rw, `{"message_id":"`+req.MessageID+`","result":null,"partial":false}`)
				return
			default:
				t.Fatalf("unexpected command over websocket: %s", req.Command)
			}
		}
	}))
	defer server.Close()

	cfg := config.DefaultConfig()
	cfg.MusicAssistant.URL = server.URL
	cfg.MusicAssistant.Token = "ma-token"
	cfg.MusicAssistant.TargetPlayerID = "player123"

	adapter := newDirectAdapter(&cfg)
	sourceURL := "http://source.local/song.mp3?token=source"
	if err := adapter.PlayMedia(PlayRequest{
		SourceURL:   sourceURL,
		ContentType: "audio/mpeg",
		Title:       "WebSocket Song",
	}); err != nil {
		t.Fatalf("PlayMedia failed: %v", err)
	}

	var playArgs map[string]any
	for len(calls) > 0 {
		call := <-calls
		if call.Command == "player_queues/play_media" {
			playArgs = call.Args
			break
		}
	}
	if playArgs == nil {
		t.Fatal("did not receive player_queues/play_media over websocket")
	}
	if playArgs["queue_id"] != "queue123" {
		t.Fatalf("unexpected queue_id: %v", playArgs["queue_id"])
	}
	media := playArgs["media"].(map[string]any)
	if media["item_id"] != sourceURL {
		t.Fatalf("play_media should send original source URL, got %v", media["item_id"])
	}
}

func websocketAccept(key string) string {
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(sum[:])
}

func readClientText(r *bufio.Reader) (string, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(r, header); err != nil {
		return "", err
	}
	opcode := header[0] & 0x0f
	if opcode == 0x8 {
		return "", io.EOF
	}
	if opcode != 0x1 {
		return "", io.ErrUnexpectedEOF
	}
	masked := header[1]&0x80 != 0
	n := uint64(header[1] & 0x7f)
	switch n {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return "", err
		}
		n = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return "", err
		}
		n = binary.BigEndian.Uint64(ext[:])
	}
	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(r, mask[:]); err != nil {
			return "", err
		}
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return "", err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return string(payload), nil
}

func writeServerText(t *testing.T, rw *bufio.ReadWriter, s string) {
	t.Helper()
	var frame strings.Builder
	frame.WriteByte(0x81)
	n := len(s)
	switch {
	case n < 126:
		frame.WriteByte(byte(n))
	case n <= 65535:
		frame.WriteByte(126)
		var ext [2]byte
		binary.BigEndian.PutUint16(ext[:], uint16(n))
		frame.Write(ext[:])
	default:
		frame.WriteByte(127)
		var ext [8]byte
		binary.BigEndian.PutUint64(ext[:], uint64(n))
		frame.Write(ext[:])
	}
	frame.WriteString(s)
	if _, err := rw.WriteString(frame.String()); err != nil {
		t.Fatalf("write websocket frame: %v", err)
	}
	if err := rw.Flush(); err != nil {
		t.Fatalf("flush websocket frame: %v", err)
	}
}
