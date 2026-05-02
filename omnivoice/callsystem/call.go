package callsystem

import (
	"context"
	"sync"
	"time"

	"github.com/plexusone/omnivoice-core/agent"
	"github.com/plexusone/omnivoice-core/callsystem"
	omnitransport "github.com/plexusone/omnivoice-core/transport"
	"github.com/team-telnyx/telnyx-go/v4"
)

// Verify interface compliance at compile time.
var _ callsystem.Call = (*Call)(nil)

// Call implements callsystem.Call for Telnyx calls.
type Call struct {
	id        string // call_control_id
	direction callsystem.CallDirection
	status    callsystem.CallStatus
	from      string
	to        string
	startTime time.Time
	provider  *Provider

	mu        sync.RWMutex
	transport omnitransport.Connection
	agent     agent.Session
}

// newCall creates a new Call instance.
func newCall(callControlID, from, to string, direction callsystem.CallDirection, provider *Provider) *Call {
	return &Call{
		id:        callControlID,
		direction: direction,
		status:    callsystem.StatusRinging,
		from:      from,
		to:        to,
		startTime: time.Now(),
		provider:  provider,
	}
}

// ID returns the call identifier (call_control_id).
func (c *Call) ID() string {
	return c.id
}

// Direction returns inbound or outbound.
func (c *Call) Direction() callsystem.CallDirection {
	return c.direction
}

// Status returns the current call status.
func (c *Call) Status() callsystem.CallStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}

// From returns the caller ID.
func (c *Call) From() string {
	return c.from
}

// To returns the called number.
func (c *Call) To() string {
	return c.to
}

// StartTime returns when the call started.
func (c *Call) StartTime() time.Time {
	return c.startTime
}

// Duration returns the call duration.
func (c *Call) Duration() time.Duration {
	return time.Since(c.startTime)
}

// Answer answers an inbound call using Telnyx Call Control API.
func (c *Call) Answer(ctx context.Context) error {
	_, err := c.provider.client.Calls.Actions.Answer(ctx, c.id, telnyx.CallActionAnswerParams{})
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.status = callsystem.StatusAnswered
	c.mu.Unlock()

	return nil
}

// Hangup ends the call using Telnyx Call Control API.
func (c *Call) Hangup(ctx context.Context) error {
	_, err := c.provider.client.Calls.Actions.Hangup(ctx, c.id, telnyx.CallActionHangupParams{})
	if err != nil {
		return err
	}

	c.mu.Lock()
	c.status = callsystem.StatusEnded
	if c.transport != nil {
		_ = c.transport.Close()
	}
	c.mu.Unlock()

	return nil
}

// Transport returns the underlying transport connection.
func (c *Call) Transport() omnitransport.Connection {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.transport
}

// SetTransport sets the transport connection (called when Media Streaming connects).
func (c *Call) SetTransport(conn omnitransport.Connection) {
	c.mu.Lock()
	c.transport = conn
	c.mu.Unlock()
}

// AttachAgent attaches a voice agent to handle the call.
func (c *Call) AttachAgent(ctx context.Context, session agent.Session) error {
	c.mu.Lock()
	c.agent = session
	c.mu.Unlock()

	return session.Start(ctx)
}

// DetachAgent detaches the voice agent.
func (c *Call) DetachAgent(ctx context.Context) error {
	c.mu.Lock()
	session := c.agent
	c.agent = nil
	c.mu.Unlock()

	if session != nil {
		return session.Stop(ctx)
	}
	return nil
}

// handleEvent updates call status based on Telnyx webhook events.
func (c *Call) handleEvent(eventType string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch eventType {
	case "call.initiated":
		c.status = callsystem.StatusRinging
	case "call.answered":
		c.status = callsystem.StatusAnswered
	case "call.hangup":
		c.status = callsystem.StatusEnded
	case "call.machine.detection.ended":
		// Machine detection completed, call may still be active
	}
}

// StartMediaStreaming initiates media streaming for the call.
// This sends the streaming:start command to enable real-time audio.
func (c *Call) StartMediaStreaming(ctx context.Context, streamURL string) error {
	_, err := c.provider.client.Calls.Actions.StartStreaming(ctx, c.id, telnyx.CallActionStartStreamingParams{
		StreamURL:   telnyx.String(streamURL),
		StreamTrack: telnyx.CallActionStartStreamingParamsStreamTrackInboundTrack,
	})
	return err
}

// StopMediaStreaming stops media streaming for the call.
func (c *Call) StopMediaStreaming(ctx context.Context) error {
	_, err := c.provider.client.Calls.Actions.StopStreaming(ctx, c.id, telnyx.CallActionStopStreamingParams{})
	return err
}

// Speak uses Telnyx's built-in TTS to speak text on the call.
// Note: Voice and language parameters are passed but may need to match Telnyx's supported values.
func (c *Call) Speak(ctx context.Context, text, voice, language string) error {
	params := telnyx.CallActionSpeakParams{
		Payload:     text,
		PayloadType: telnyx.CallActionSpeakParamsPayloadTypeText,
	}

	_, err := c.provider.client.Calls.Actions.Speak(ctx, c.id, params)
	return err
}

// PlayAudio plays an audio file URL on the call.
func (c *Call) PlayAudio(ctx context.Context, audioURL string) error {
	_, err := c.provider.client.Calls.Actions.StartPlayback(ctx, c.id, telnyx.CallActionStartPlaybackParams{
		AudioURL: telnyx.String(audioURL),
	})
	return err
}

// StopAudio stops any currently playing audio.
func (c *Call) StopAudio(ctx context.Context) error {
	_, err := c.provider.client.Calls.Actions.StopPlayback(ctx, c.id, telnyx.CallActionStopPlaybackParams{})
	return err
}

// StartTranscription starts real-time transcription for the call.
func (c *Call) StartTranscription(ctx context.Context, language string) error {
	params := telnyx.CallActionStartTranscriptionParams{}

	_, err := c.provider.client.Calls.Actions.StartTranscription(ctx, c.id, params)
	return err
}

// StopTranscription stops real-time transcription.
func (c *Call) StopTranscription(ctx context.Context) error {
	_, err := c.provider.client.Calls.Actions.StopTranscription(ctx, c.id, telnyx.CallActionStopTranscriptionParams{})
	return err
}
