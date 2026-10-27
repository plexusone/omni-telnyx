package gateway

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/team-telnyx/telnyx-go/v4"
	"github.com/team-telnyx/telnyx-go/v4/option"

	"github.com/plexusone/omnivoice-core/audio/format"
	coregateway "github.com/plexusone/omnivoice-core/gateway"
)

const testCallID = "v3:test-call"

func newTestGateway(t *testing.T, mode coregateway.PipelineMode) *Gateway {
	t.Helper()
	g, err := New(Config{
		APIKey:      "test-key",
		PhoneNumber: "+15550000000",
		PublicURL:   "https://example.com",
		Mode:        mode,
	})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	return g
}

// dialMediaStream connects to the gateway's media stream handler as Telnyx
// would.
func dialMediaStream(t *testing.T, g *Gateway) *websocket.Conn {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(g.handleMediaStream))
	t.Cleanup(srv.Close)

	ws, resp, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial media stream: %v", err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatalf("close handshake body: %v", err)
	}
	t.Cleanup(func() {
		// The gateway may already have closed the connection.
		if err := ws.Close(); err != nil {
			t.Logf("close websocket: %v", err)
		}
	})
	return ws
}

func sendJSON(t *testing.T, ws *websocket.Conn, msg any) {
	t.Helper()
	if err := ws.WriteJSON(msg); err != nil {
		t.Fatalf("write %v: %v", msg, err)
	}
}

func waitEvent(t *testing.T, sess coregateway.Session, want coregateway.EventType) {
	t.Helper()
	timeout := time.After(2 * time.Second)
	for {
		select {
		case ev, ok := <-sess.Events():
			if !ok {
				t.Fatalf("events closed before %s", want)
			}
			if ev.Type == want {
				return
			}
		case <-timeout:
			t.Fatalf("timed out waiting for %s", want)
		}
	}
}

func readMessage(t *testing.T, ws *websocket.Conn) map[string]any {
	t.Helper()
	if err := ws.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	_, data, err := ws.ReadMessage()
	if err != nil {
		t.Fatalf("read message: %v", err)
	}
	var msg map[string]any
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("decode message: %v", err)
	}
	return msg
}

func TestAudioModeSessionCarriesCallAudio(t *testing.T) {
	g := newTestGateway(t, coregateway.PipelineModeAudio)
	sess, ok := g.publicSession(g.createSession(testCallID, "+15550000000", "+16695550100", "outbound")).(coregateway.AudioSession)
	if !ok {
		t.Fatal("audio mode session does not implement AudioSession")
	}
	if sess.AudioFormat() != format.Telnyx {
		t.Fatalf("AudioFormat() = %+v, want Telnyx mu-law 8 kHz", sess.AudioFormat())
	}
	if err := sess.SendAudio([]byte{1}); !errors.Is(err, coregateway.ErrAudioNotConnected) {
		t.Fatalf("SendAudio before connect = %v, want ErrAudioNotConnected", err)
	}

	ws := dialMediaStream(t, g)
	sendJSON(t, ws, map[string]any{"event": "connected"})
	sendJSON(t, ws, map[string]any{
		"event": "start",
		"start": map[string]any{
			"stream_id":       "stream-1",
			"call_control_id": testCallID,
			"media_format":    map[string]any{"encoding": "audio/x-mulaw", "sample_rate": 8000, "channels": 1},
		},
	})
	waitEvent(t, sess, coregateway.EventSessionStarted)

	// Remote audio reaches the application; the agent's own echo does not.
	sendJSON(t, ws, map[string]any{"event": "media", "media": map[string]any{
		"track": "outbound", "payload": base64.StdEncoding.EncodeToString([]byte{0x11}),
	}})
	sendJSON(t, ws, map[string]any{"event": "media", "media": map[string]any{
		"track": "inbound", "payload": base64.StdEncoding.EncodeToString([]byte{0x7f, 0x00, 0xff}),
	}})
	select {
	case got := <-sess.ReceiveAudio():
		if string(got) != string([]byte{0x7f, 0x00, 0xff}) {
			t.Fatalf("ReceiveAudio() = %v, want the inbound chunk", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for remote audio")
	}

	// Application audio is sent with increasing sequence numbers.
	for i, chunk := range [][]byte{{0x01}, {0x02}} {
		if err := sess.SendAudio(chunk); err != nil {
			t.Fatalf("SendAudio() error: %v", err)
		}
		msg := readMessage(t, ws)
		media, _ := msg["media"].(map[string]any)
		if msg["event"] != "media" || msg["stream_id"] != "stream-1" ||
			msg["sequence_number"] != float64(i+1) ||
			media["payload"] != base64.StdEncoding.EncodeToString(chunk) {
			t.Fatalf("sent message %d = %v", i, msg)
		}
	}

	if err := sess.ClearAudio(); err != nil {
		t.Fatalf("ClearAudio() error: %v", err)
	}
	if msg := readMessage(t, ws); msg["event"] != "clear" {
		t.Fatalf("clear message = %v", msg)
	}

	sendJSON(t, ws, map[string]any{"event": "stop", "stop": map[string]any{"call_control_id": testCallID}})
	waitEvent(t, sess, coregateway.EventSessionEnded)
	if _, ok := <-sess.ReceiveAudio(); ok {
		t.Fatal("ReceiveAudio() open after stop")
	}
	if _, ok := g.GetSession(testCallID); ok {
		t.Fatal("session still registered after the stream stopped")
	}
	if err := sess.SendAudio([]byte{1}); !errors.Is(err, coregateway.ErrSessionClosed) {
		t.Fatalf("SendAudio after stop = %v, want ErrSessionClosed", err)
	}
}

func TestOtherModesAreNotAudioSessions(t *testing.T) {
	for _, mode := range []coregateway.PipelineMode{"", coregateway.PipelineModeText, coregateway.PipelineModeRealtime} {
		g := newTestGateway(t, mode)
		g.createSession(testCallID, "", "", "outbound")
		sess, ok := g.GetSession(testCallID)
		if !ok {
			t.Fatalf("mode %q: session not found", mode)
		}
		if _, isAudio := sess.(coregateway.AudioSession); isAudio {
			t.Fatalf("mode %q: session implements AudioSession", mode)
		}
	}
}

// fakeCallControl records Telnyx Call Control requests by path.
type fakeCallControl struct {
	mu     sync.Mutex
	bodies map[string]map[string]any
}

func (f *fakeCallControl) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	body := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}
	f.mu.Lock()
	f.bodies[r.URL.Path] = body
	f.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	resp := `{"data":{"result":"ok"}}`
	if r.URL.Path == "/calls" {
		resp = `{"data":{"call_control_id":"` + testCallID + `"}}`
	}
	if _, err := io.WriteString(w, resp); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (f *fakeCallControl) body(path string) (map[string]any, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.bodies[path]
	return b, ok
}

// answerOutboundCall places a call through the gateway against a fake Call
// Control API, delivers its call.answered webhook, and returns the
// streaming_start request body.
func answerOutboundCall(t *testing.T, mode coregateway.PipelineMode) map[string]any {
	t.Helper()
	fake := &fakeCallControl{bodies: map[string]map[string]any{}}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)

	g := newTestGateway(t, mode)
	*g.callSystem.Client() = telnyx.NewClient(
		option.WithAPIKey("test-key"),
		option.WithBaseURL(srv.URL+"/"),
		option.WithMaxRetries(0),
	)
	if _, err := g.MakeCallWithOptions(context.Background(), "+16695550100"); err != nil {
		t.Fatalf("MakeCallWithOptions() error: %v", err)
	}

	webhook := `{"data":{"event_type":"call.answered","payload":{"call_control_id":"` + testCallID + `","direction":"outgoing"}}}`
	rec := httptest.NewRecorder()
	g.handleInboundCall(rec, httptest.NewRequest(http.MethodPost, "/voice/inbound", strings.NewReader(webhook)))
	if rec.Code != http.StatusOK {
		t.Fatalf("webhook status = %d", rec.Code)
	}

	start, ok := fake.body("/calls/" + testCallID + "/actions/streaming_start")
	if !ok {
		t.Fatal("call.answered did not start media streaming")
	}
	return start
}

func TestAudioModeRequestsPCMUStreaming(t *testing.T) {
	start := answerOutboundCall(t, coregateway.PipelineModeAudio)
	if start["stream_bidirectional_mode"] != "rtp" || start["stream_bidirectional_codec"] != "PCMU" {
		t.Fatalf("streaming_start = %v, want bidirectional rtp with PCMU", start)
	}
	if start["stream_url"] != "wss://example.com/ws/media-stream" {
		t.Fatalf("stream_url = %v", start["stream_url"])
	}
}

func TestTextModeStreamingUnchanged(t *testing.T) {
	start := answerOutboundCall(t, coregateway.PipelineModeText)
	if _, ok := start["stream_bidirectional_mode"]; ok {
		t.Fatalf("text mode streaming_start = %v, want no bidirectional mode", start)
	}
}
