// Package transport provides a Telnyx Media Streaming implementation of transport.Transport.
package transport

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/plexusone/omnivoice-core/transport"
)

// Verify interface compliance at compile time.
var (
	_ transport.Transport          = (*Provider)(nil)
	_ transport.TelephonyTransport = (*Provider)(nil)
)

// Provider implements transport.Transport using Telnyx Media Streaming.
type Provider struct {
	mu          sync.RWMutex
	connections map[string]*Connection
	listeners   map[string]chan transport.Connection
	dtmfHandler func(conn transport.Connection, digit string)
}

// Option configures the Provider.
type Option func(*options)

type options struct{}

// New creates a new Telnyx Media Streaming transport provider.
func New(opts ...Option) (*Provider, error) {
	return &Provider{
		connections: make(map[string]*Connection),
		listeners:   make(map[string]chan transport.Connection),
	}, nil
}

// Name returns the transport name.
func (p *Provider) Name() string {
	return "telnyx-media-streaming"
}

// Protocol returns the protocol type.
func (p *Provider) Protocol() string {
	return "websocket"
}

// Listen starts listening for incoming Media Streaming connections.
// The addr should be the path to handle (e.g., "/media-stream").
func (p *Provider) Listen(ctx context.Context, addr string) (<-chan transport.Connection, error) {
	connCh := make(chan transport.Connection, 10)

	p.mu.Lock()
	p.listeners[addr] = connCh
	p.mu.Unlock()

	return connCh, nil
}

// HandleWebSocket handles an incoming WebSocket connection from Telnyx.
// This should be called from your HTTP WebSocket handler.
func (p *Provider) HandleWebSocket(w http.ResponseWriter, r *http.Request, listenerPath string) error {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}

	wsConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return fmt.Errorf("websocket upgrade failed: %w", err)
	}

	conn := &Connection{
		id:       "", // Will be set from start message
		wsConn:   wsConn,
		provider: p,
		events:   make(chan transport.Event, 100),
		audioIn:  newAudioWriter(),
		audioOut: newAudioReader(),
		done:     make(chan struct{}),
	}

	// Start read/write loops
	go conn.readLoop()
	go conn.writeLoop()

	// Notify listener
	p.mu.RLock()
	listener, ok := p.listeners[listenerPath]
	p.mu.RUnlock()

	if ok {
		select {
		case listener <- conn:
		default:
		}
	}

	return nil
}

// Connect initiates an outbound connection (not typically used for Media Streaming).
func (p *Provider) Connect(ctx context.Context, addr string, config transport.Config) (transport.Connection, error) {
	return nil, fmt.Errorf("outbound connections not supported for Media Streaming; use CallSystem.MakeCall instead")
}

// Close shuts down the transport.
func (p *Provider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, conn := range p.connections {
		_ = conn.Close()
	}

	for _, ch := range p.listeners {
		close(ch)
	}

	p.connections = make(map[string]*Connection)
	p.listeners = make(map[string]chan transport.Connection)

	return nil
}

// SendDTMF sends DTMF tones.
func (p *Provider) SendDTMF(conn transport.Connection, digits string) error {
	return fmt.Errorf("DTMF sending not supported via Media Streaming; use Call Control API")
}

// OnDTMF sets the DTMF handler.
func (p *Provider) OnDTMF(handler func(conn transport.Connection, digit string)) {
	p.mu.Lock()
	p.dtmfHandler = handler
	p.mu.Unlock()
}

// Transfer transfers the call.
func (p *Provider) Transfer(conn transport.Connection, target string) error {
	return fmt.Errorf("transfer not implemented; use CallSystem to transfer the call")
}

// Hold places the call on hold.
func (p *Provider) Hold(conn transport.Connection) error {
	return fmt.Errorf("hold not implemented; use CallSystem to hold the call")
}

// Unhold resumes a held call.
func (p *Provider) Unhold(conn transport.Connection) error {
	return fmt.Errorf("unhold not implemented; use CallSystem to unhold the call")
}

// GetConnection returns a connection by call control ID.
func (p *Provider) GetConnection(callControlID string) (*Connection, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	conn, ok := p.connections[callControlID]
	return conn, ok
}

// Connection implements transport.Connection for Telnyx Media Streaming.
type Connection struct {
	id          string // call_control_id
	streamID    string
	wsConn      *websocket.Conn
	provider    *Provider
	events      chan transport.Event
	audioIn     *audioWriter
	audioOut    *audioReader
	done        chan struct{}
	mu          sync.RWMutex
	closed      bool
	closeOnce   sync.Once
	remoteAddr  net.Addr
	sequenceNum int64
}

// ID returns the connection identifier (call_control_id).
func (c *Connection) ID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.id
}

// StreamID returns the stream identifier.
func (c *Connection) StreamID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.streamID
}

// AudioIn returns a writer for sending audio to Telnyx.
func (c *Connection) AudioIn() io.WriteCloser {
	return c.audioIn
}

// AudioOut returns a reader for receiving audio from Telnyx.
func (c *Connection) AudioOut() io.Reader {
	return c.audioOut
}

// Events returns a channel for transport events.
func (c *Connection) Events() <-chan transport.Event {
	return c.events
}

// Close closes the connection.
func (c *Connection) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()

		close(c.done)
		_ = c.audioIn.Close()
		c.audioOut.close()
		close(c.events)
		_ = c.wsConn.Close()

		c.provider.mu.Lock()
		delete(c.provider.connections, c.id)
		c.provider.mu.Unlock()
	})
	return nil
}

// RemoteAddr returns the remote address.
func (c *Connection) RemoteAddr() net.Addr {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.remoteAddr
}

// Telnyx Media Streaming message types.
// See: https://developers.telnyx.com/docs/voice/media-streaming

type streamMessage struct {
	Event          string        `json:"event"`
	StreamID       string        `json:"stream_id,omitempty"`
	CallControlID  string        `json:"call_control_id,omitempty"`
	Media          *mediaPayload `json:"media,omitempty"`
	Start          *startPayload `json:"start,omitempty"`
	Stop           *stopPayload  `json:"stop,omitempty"`
	DTMF           *dtmfPayload  `json:"dtmf,omitempty"`
	SequenceNumber int64         `json:"sequence_number,omitempty"`
}

type startPayload struct {
	StreamID      string            `json:"stream_id"`
	CallControlID string            `json:"call_control_id"`
	CallLegID     string            `json:"call_leg_id"`
	CustomParams  map[string]string `json:"custom_parameters,omitempty"`
	MediaFormat   mediaFormat       `json:"media_format"`
}

type mediaFormat struct {
	Encoding   string `json:"encoding"` // "audio/x-mulaw" or "audio/x-alaw"
	SampleRate int    `json:"sample_rate"`
	Channels   int    `json:"channels"`
}

type mediaPayload struct {
	Track   string `json:"track"` // "inbound" or "outbound"
	Chunk   int64  `json:"chunk"`
	Payload string `json:"payload"` // Base64 encoded audio
}

type stopPayload struct {
	CallControlID string `json:"call_control_id"`
	Reason        string `json:"reason,omitempty"`
}

type dtmfPayload struct {
	Digit    string `json:"digit"`
	Duration int    `json:"duration_millis,omitempty"`
}

// readLoop reads messages from the WebSocket.
func (c *Connection) readLoop() {
	defer func() { _ = c.Close() }()

	for {
		select {
		case <-c.done:
			return
		default:
		}

		_, data, err := c.wsConn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				c.events <- transport.Event{Type: transport.EventError, Error: err}
			}
			return
		}

		var msg streamMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}

		switch msg.Event {
		case "connected":
			c.events <- transport.Event{Type: transport.EventConnected}

		case "start":
			if msg.Start != nil {
				c.mu.Lock()
				c.id = msg.Start.CallControlID
				c.streamID = msg.Start.StreamID
				c.mu.Unlock()

				c.provider.mu.Lock()
				c.provider.connections[c.id] = c
				c.provider.mu.Unlock()

				c.events <- transport.Event{Type: transport.EventAudioStarted}
			}

		case "media":
			if msg.Media != nil && msg.Media.Payload != "" {
				// Only process inbound audio (from caller)
				if msg.Media.Track == "inbound" {
					audio, err := base64.StdEncoding.DecodeString(msg.Media.Payload)
					if err != nil {
						continue
					}
					c.audioOut.write(audio)
				}
			}

		case "dtmf":
			if msg.DTMF != nil {
				c.events <- transport.Event{
					Type: transport.EventDTMF,
					Data: msg.DTMF.Digit,
				}

				c.provider.mu.RLock()
				handler := c.provider.dtmfHandler
				c.provider.mu.RUnlock()

				if handler != nil {
					handler(c, msg.DTMF.Digit)
				}
			}

		case "stop":
			c.events <- transport.Event{Type: transport.EventAudioStopped}
			c.events <- transport.Event{Type: transport.EventDisconnected}
			return
		}
	}
}

// writeLoop writes audio to the WebSocket.
func (c *Connection) writeLoop() {
	for {
		select {
		case <-c.done:
			return
		case audio := <-c.audioIn.ch:
			c.mu.Lock()
			c.sequenceNum++
			seqNum := c.sequenceNum
			streamID := c.streamID
			c.mu.Unlock()

			// Encode audio to base64
			encoded := base64.StdEncoding.EncodeToString(audio)

			msg := map[string]any{
				"event":           "media",
				"stream_id":       streamID,
				"sequence_number": seqNum,
				"media": map[string]any{
					"track":   "outbound",
					"payload": encoded,
				},
			}

			c.mu.RLock()
			closed := c.closed
			c.mu.RUnlock()

			if !closed {
				if err := c.wsConn.WriteJSON(msg); err != nil {
					return
				}
			}
		}
	}
}

// SendMark sends a mark message for synchronization.
func (c *Connection) SendMark(name string) error {
	c.mu.RLock()
	streamID := c.streamID
	c.mu.RUnlock()

	msg := map[string]any{
		"event":     "mark",
		"stream_id": streamID,
		"mark": map[string]string{
			"name": name,
		},
	}
	return c.wsConn.WriteJSON(msg)
}

// Clear clears the audio buffer.
func (c *Connection) Clear() error {
	c.mu.RLock()
	streamID := c.streamID
	c.mu.RUnlock()

	msg := map[string]any{
		"event":     "clear",
		"stream_id": streamID,
	}
	return c.wsConn.WriteJSON(msg)
}

// audioWriter implements io.WriteCloser for sending audio.
type audioWriter struct {
	ch     chan []byte
	closed bool
	mu     sync.Mutex
}

func newAudioWriter() *audioWriter {
	return &audioWriter{
		ch: make(chan []byte, 100),
	}
}

func (w *audioWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return 0, io.ErrClosedPipe
	}

	// Copy the data
	data := make([]byte, len(p))
	copy(data, p)

	select {
	case w.ch <- data:
		return len(p), nil
	default:
		// Buffer full, drop oldest
		select {
		case <-w.ch:
		default:
		}
		w.ch <- data
		return len(p), nil
	}
}

func (w *audioWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.closed {
		w.closed = true
		close(w.ch)
	}
	return nil
}

// audioReader implements io.Reader for receiving audio.
type audioReader struct {
	ch     chan []byte
	buffer []byte
	mu     sync.Mutex
}

func newAudioReader() *audioReader {
	return &audioReader{
		ch: make(chan []byte, 100),
	}
}

func (r *audioReader) Read(p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If we have buffered data, return it first
	if len(r.buffer) > 0 {
		n = copy(p, r.buffer)
		r.buffer = r.buffer[n:]
		return n, nil
	}

	// Wait for new data
	data, ok := <-r.ch
	if !ok {
		return 0, io.EOF
	}

	n = copy(p, data)
	if n < len(data) {
		r.buffer = data[n:]
	}
	return n, nil
}

func (r *audioReader) write(data []byte) {
	select {
	case r.ch <- data:
	default:
		// Buffer full, drop
	}
}

func (r *audioReader) close() {
	close(r.ch)
}
