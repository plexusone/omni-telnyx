package gateway

import (
	"encoding/base64"
	"encoding/json"
	"sync"

	"github.com/gorilla/websocket"
)

// mediaStreamConn wraps a WebSocket connection for Telnyx Media Streaming.
type mediaStreamConn struct {
	wsConn   *websocket.Conn
	gateway  *Gateway
	session  *Session
	events   chan Event
	audioIn  chan []byte // Audio from Telnyx (user speech)
	audioOut chan []byte // Audio to Telnyx (agent speech)
	done     chan struct{}

	sessionReady  chan struct{}
	pipelineReady chan struct{} // Signaled when pipeline starts reading
	streamID      string
	callControlID string

	mu        sync.RWMutex
	closed    bool
	closeOnce sync.Once
}

// Telnyx Media Streaming message types.
// See: https://developers.telnyx.com/docs/voice/media-streaming
type mediaMessage struct {
	Event          string        `json:"event"`
	SequenceNumber int64         `json:"sequence_number,omitempty"`
	StreamID       string        `json:"stream_id,omitempty"`
	CallControlID  string        `json:"call_control_id,omitempty"`
	Start          *startMessage `json:"start,omitempty"`
	Media          *mediaPayload `json:"media,omitempty"`
	Mark           *markMessage  `json:"mark,omitempty"`
	Stop           *stopMessage  `json:"stop,omitempty"`
	DTMF           *dtmfMessage  `json:"dtmf,omitempty"`
}

type startMessage struct {
	StreamID      string            `json:"stream_id"`
	CallControlID string            `json:"call_control_id"`
	CallLegID     string            `json:"call_leg_id"`
	MediaFormat   mediaFormat       `json:"media_format"`
	CustomParams  map[string]string `json:"custom_parameters"`
}

type mediaFormat struct {
	Encoding   string `json:"encoding"`    // "audio/x-mulaw" or "audio/x-alaw"
	SampleRate int    `json:"sample_rate"` // 8000
	Channels   int    `json:"channels"`    // 1
}

type mediaPayload struct {
	Track   string `json:"track"` // "inbound" or "outbound"
	Chunk   int64  `json:"chunk"`
	Payload string `json:"payload"` // Base64 encoded audio
}

type markMessage struct {
	Name string `json:"name"`
}

type stopMessage struct {
	CallControlID string `json:"call_control_id"`
	Reason        string `json:"reason,omitempty"`
}

type dtmfMessage struct {
	Digit    string `json:"digit"`
	Duration int    `json:"duration_millis,omitempty"`
}

// readLoop reads messages from the WebSocket.
func (c *mediaStreamConn) readLoop() {
	defer func() {
		c.close()
	}()

	for {
		select {
		case <-c.done:
			return
		default:
		}

		_, data, err := c.wsConn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				c.gateway.logger.Debug("websocket read error", "error", err)
			}
			return
		}

		var msg mediaMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			c.gateway.logger.Debug("failed to parse media message", "error", err)
			continue
		}

		switch msg.Event {
		case "connected":
			c.gateway.logger.Debug("media stream connected")

		case "start":
			if msg.Start != nil {
				c.mu.Lock()
				c.streamID = msg.Start.StreamID
				c.callControlID = msg.Start.CallControlID
				c.mu.Unlock()

				c.gateway.logger.Info("media stream started",
					"stream_id", msg.Start.StreamID,
					"call_control_id", msg.Start.CallControlID,
					"encoding", msg.Start.MediaFormat.Encoding,
					"sample_rate", msg.Start.MediaFormat.SampleRate)

				// Find the session by call control ID
				session, ok := c.gateway.getSessionInternal(msg.Start.CallControlID)
				if !ok {
					// Create session if not exists (for outbound calls)
					from := msg.Start.CustomParams["from"]
					to := msg.Start.CustomParams["to"]
					session = c.gateway.createSession(msg.Start.CallControlID, from, to, "outbound")
				}

				c.session = session

				// Signal that session is ready
				close(c.sessionReady)
			}

		case "media":
			if msg.Media != nil && msg.Media.Payload != "" {
				// Only process inbound audio (from caller)
				if msg.Media.Track != "inbound" {
					continue
				}

				// Only process audio after session is ready
				if c.session == nil {
					continue
				}

				// Wait for pipeline to be ready before sending audio
				select {
				case <-c.pipelineReady:
					// Pipeline is ready to receive audio
				default:
					// Pipeline not ready yet, skip this audio chunk
					continue
				}

				// Decode base64 audio
				audio, err := base64.StdEncoding.DecodeString(msg.Media.Payload)
				if err != nil {
					c.gateway.logger.Debug("failed to decode audio", "error", err)
					continue
				}

				// Send to audio input channel
				select {
				case c.audioIn <- audio:
				default:
					// Channel full, drop audio (shouldn't happen often now)
				}
			}

		case "dtmf":
			if msg.DTMF != nil {
				c.gateway.logger.Debug("received DTMF", "digit", msg.DTMF.Digit)
				if c.session != nil {
					c.session.emitEvent(EventType("dtmf"), msg.DTMF.Digit)
				}
			}

		case "stop":
			c.gateway.logger.Info("media stream stopped",
				"call_control_id", c.callControlID)
			return

		case "mark":
			// Mark event - used for synchronization
			if msg.Mark != nil {
				c.gateway.logger.Debug("received mark", "name", msg.Mark.Name)
			}
		}
	}
}

// writeLoop writes audio to the WebSocket.
func (c *mediaStreamConn) writeLoop() {
	seqNum := int64(0)
	for {
		select {
		case <-c.done:
			return
		case audio := <-c.audioOut:
			seqNum++
			if err := c.sendAudio(audio, seqNum); err != nil {
				c.gateway.logger.Debug("failed to send audio", "error", err)
				return
			}
		}
	}
}

// sendAudio sends audio data to Telnyx.
func (c *mediaStreamConn) sendAudio(audio []byte, seqNum int64) error {
	c.mu.RLock()
	streamID := c.streamID
	closed := c.closed
	c.mu.RUnlock()

	if closed || streamID == "" {
		return nil
	}

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

	return c.wsConn.WriteJSON(msg)
}

// sendMark sends a mark message for synchronization.
//
//nolint:unused // Reserved for barge-in detection and audio sync
func (c *mediaStreamConn) sendMark(name string) error {
	c.mu.RLock()
	streamID := c.streamID
	c.mu.RUnlock()

	if streamID == "" {
		return nil
	}

	msg := map[string]any{
		"event":     "mark",
		"stream_id": streamID,
		"mark": map[string]string{
			"name": name,
		},
	}

	return c.wsConn.WriteJSON(msg)
}

// clear sends a clear message to clear the audio buffer.
func (c *mediaStreamConn) clear() error {
	c.mu.RLock()
	streamID := c.streamID
	c.mu.RUnlock()

	if streamID == "" {
		return nil
	}

	msg := map[string]any{
		"event":     "clear",
		"stream_id": streamID,
	}

	return c.wsConn.WriteJSON(msg)
}

// close closes the connection.
func (c *mediaStreamConn) close() {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()

		close(c.done)
		close(c.audioIn)
		close(c.audioOut)
		_ = c.wsConn.Close()
	})
}

// AudioIn returns the channel for receiving audio from Telnyx.
func (c *mediaStreamConn) AudioIn() <-chan []byte {
	return c.audioIn
}

// AudioOut returns the channel for sending audio to Telnyx.
func (c *mediaStreamConn) AudioOut() chan<- []byte {
	return c.audioOut
}
