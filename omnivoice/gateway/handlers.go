package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	telnyxcall "github.com/plexusone/omni-telnyx/omnivoice/callsystem"
)

// Telnyx webhook event structure.
// See: https://developers.telnyx.com/docs/v2/call-control/receiving-events
type webhookEvent struct {
	Data struct {
		EventType string `json:"event_type"`
		ID        string `json:"id"`
		Payload   struct {
			CallControlID string            `json:"call_control_id"`
			CallLegID     string            `json:"call_leg_id"`
			CallSessionID string            `json:"call_session_id"`
			ConnectionID  string            `json:"connection_id"`
			From          string            `json:"from"`
			To            string            `json:"to"`
			Direction     string            `json:"direction"`
			State         string            `json:"state"`
			StartTime     string            `json:"start_time"`
			EndTime       string            `json:"end_time"`
			HangupCause   string            `json:"hangup_cause"`
			HangupSource  string            `json:"hangup_source"`
			StreamID      string            `json:"stream_id,omitempty"`
			CustomParams  map[string]string `json:"custom_parameters,omitempty"`
		} `json:"payload"`
		OccurredAt string `json:"occurred_at"`
	} `json:"data"`
	Meta struct {
		AttemptNumber int    `json:"attempt_number"`
		DeliveredTo   string `json:"delivered_to"`
	} `json:"meta"`
}

// handleInboundCall handles incoming Telnyx webhook for new calls.
// Telnyx sends JSON payloads to webhooks, unlike Twilio's form data.
func (g *Gateway) handleInboundCall(w http.ResponseWriter, r *http.Request) {
	// Limit request body size to prevent memory exhaustion
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10) // 64KB

	var event webhookEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		g.logger.Error("failed to parse webhook", "error", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	eventType := event.Data.EventType
	payload := event.Data.Payload

	g.logger.Info("received webhook",
		"event_type", eventType,
		"call_control_id", sanitize(payload.CallControlID),
		"from", sanitize(payload.From),
		"to", sanitize(payload.To),
		"direction", sanitize(payload.Direction))

	switch eventType {
	case "call.initiated":
		g.handleCallInitiated(w, payload)
	case "call.answered":
		g.handleCallAnswered(w, payload)
	case "call.hangup":
		g.handleCallHangup(w, payload)
	case "call.machine.detection.ended":
		// Machine detection completed
		w.WriteHeader(http.StatusOK)
	case "streaming.started":
		g.handleStreamingStarted(w, payload)
	case "streaming.stopped":
		g.handleStreamingStopped(w, payload)
	default:
		g.logger.Debug("unhandled event type", "event_type", eventType)
		w.WriteHeader(http.StatusOK)
	}
}

// handleCallInitiated processes call.initiated events (new incoming call).
func (g *Gateway) handleCallInitiated(w http.ResponseWriter, payload struct {
	CallControlID string            `json:"call_control_id"`
	CallLegID     string            `json:"call_leg_id"`
	CallSessionID string            `json:"call_session_id"`
	ConnectionID  string            `json:"connection_id"`
	From          string            `json:"from"`
	To            string            `json:"to"`
	Direction     string            `json:"direction"`
	State         string            `json:"state"`
	StartTime     string            `json:"start_time"`
	EndTime       string            `json:"end_time"`
	HangupCause   string            `json:"hangup_cause"`
	HangupSource  string            `json:"hangup_source"`
	StreamID      string            `json:"stream_id,omitempty"`
	CustomParams  map[string]string `json:"custom_parameters,omitempty"`
}) {
	callControlID := payload.CallControlID
	from := payload.From
	to := payload.To
	direction := payload.Direction

	// Create session
	session := g.createSession(callControlID, from, to, direction)

	// Check if call should be accepted
	g.mu.RLock()
	handler := g.callHandler
	g.mu.RUnlock()

	if handler != nil {
		callInfo := &CallInfo{
			CallID:    callControlID,
			From:      from,
			To:        to,
			Direction: direction,
			StartTime: session.startTime,
		}
		if err := handler(callInfo); err != nil {
			g.logger.Info("call rejected by handler", "call_id", callControlID, "error", err)
			g.removeSession(callControlID)
			// Reject the call via Call Control API
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			call, _ := g.callSystem.GetCall(ctx, callControlID)
			if call != nil {
				_ = call.Hangup(ctx)
			}
			w.WriteHeader(http.StatusOK)
			return
		}
	}

	// Answer the call via Call Control API
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Register the call with our call system
	call, err := g.callSystem.HandleIncomingWebhook(callControlID, from, to)
	if err != nil {
		g.logger.Error("failed to handle incoming call", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// Answer the call
	if err := call.Answer(ctx); err != nil {
		g.logger.Error("failed to answer call", "error", err, "call_id", callControlID)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	g.logger.Info("call answered", "call_id", callControlID)
	w.WriteHeader(http.StatusOK)
}

// handleCallAnswered processes call.answered events.
func (g *Gateway) handleCallAnswered(w http.ResponseWriter, payload struct {
	CallControlID string            `json:"call_control_id"`
	CallLegID     string            `json:"call_leg_id"`
	CallSessionID string            `json:"call_session_id"`
	ConnectionID  string            `json:"connection_id"`
	From          string            `json:"from"`
	To            string            `json:"to"`
	Direction     string            `json:"direction"`
	State         string            `json:"state"`
	StartTime     string            `json:"start_time"`
	EndTime       string            `json:"end_time"`
	HangupCause   string            `json:"hangup_cause"`
	HangupSource  string            `json:"hangup_source"`
	StreamID      string            `json:"stream_id,omitempty"`
	CustomParams  map[string]string `json:"custom_parameters,omitempty"`
}) {
	callControlID := payload.CallControlID

	g.logger.Info("call answered event received", "call_id", callControlID)

	// Start media streaming on the call
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	call, err := g.callSystem.GetCall(ctx, callControlID)
	if err != nil {
		g.logger.Error("call not found", "error", err, "call_id", callControlID)
		w.WriteHeader(http.StatusOK)
		return
	}

	// Build WebSocket URL for Media Streaming
	wsURL := g.config.PublicURL + "/ws/media-stream"
	// Convert https:// to wss://
	wsURL = strings.Replace(wsURL, "https://", "wss://", 1)
	wsURL = strings.Replace(wsURL, "http://", "ws://", 1)

	// Start media streaming
	telnyxCall, ok := call.(*telnyxcall.Call)
	if !ok {
		g.logger.Error("unexpected call type", "call_id", callControlID)
		w.WriteHeader(http.StatusOK)
		return
	}

	if err := telnyxCall.StartMediaStreaming(ctx, wsURL); err != nil {
		g.logger.Error("failed to start media streaming", "error", err, "call_id", callControlID)
		w.WriteHeader(http.StatusOK)
		return
	}

	g.logger.Info("media streaming started", "call_id", callControlID, "ws_url", wsURL)
	w.WriteHeader(http.StatusOK)
}

// handleCallHangup processes call.hangup events.
func (g *Gateway) handleCallHangup(w http.ResponseWriter, payload struct {
	CallControlID string            `json:"call_control_id"`
	CallLegID     string            `json:"call_leg_id"`
	CallSessionID string            `json:"call_session_id"`
	ConnectionID  string            `json:"connection_id"`
	From          string            `json:"from"`
	To            string            `json:"to"`
	Direction     string            `json:"direction"`
	State         string            `json:"state"`
	StartTime     string            `json:"start_time"`
	EndTime       string            `json:"end_time"`
	HangupCause   string            `json:"hangup_cause"`
	HangupSource  string            `json:"hangup_source"`
	StreamID      string            `json:"stream_id,omitempty"`
	CustomParams  map[string]string `json:"custom_parameters,omitempty"`
}) {
	callControlID := payload.CallControlID

	g.logger.Info("call hangup",
		"call_id", callControlID,
		"cause", sanitize(payload.HangupCause),
		"source", sanitize(payload.HangupSource))

	// Update call system
	g.callSystem.HandleCallEvent(callControlID, "call.hangup")

	// Clean up session
	if session, ok := g.getSessionInternal(callControlID); ok {
		session.emitEvent(EventSessionEnded, map[string]string{
			"cause":  payload.HangupCause,
			"source": payload.HangupSource,
		})
		_ = session.Close()
	}

	w.WriteHeader(http.StatusOK)
}

// handleStreamingStarted processes streaming.started events.
func (g *Gateway) handleStreamingStarted(w http.ResponseWriter, payload struct {
	CallControlID string            `json:"call_control_id"`
	CallLegID     string            `json:"call_leg_id"`
	CallSessionID string            `json:"call_session_id"`
	ConnectionID  string            `json:"connection_id"`
	From          string            `json:"from"`
	To            string            `json:"to"`
	Direction     string            `json:"direction"`
	State         string            `json:"state"`
	StartTime     string            `json:"start_time"`
	EndTime       string            `json:"end_time"`
	HangupCause   string            `json:"hangup_cause"`
	HangupSource  string            `json:"hangup_source"`
	StreamID      string            `json:"stream_id,omitempty"`
	CustomParams  map[string]string `json:"custom_parameters,omitempty"`
}) {
	g.logger.Info("streaming started",
		"call_id", payload.CallControlID,
		"stream_id", payload.StreamID)
	w.WriteHeader(http.StatusOK)
}

// handleStreamingStopped processes streaming.stopped events.
func (g *Gateway) handleStreamingStopped(w http.ResponseWriter, payload struct {
	CallControlID string            `json:"call_control_id"`
	CallLegID     string            `json:"call_leg_id"`
	CallSessionID string            `json:"call_session_id"`
	ConnectionID  string            `json:"connection_id"`
	From          string            `json:"from"`
	To            string            `json:"to"`
	Direction     string            `json:"direction"`
	State         string            `json:"state"`
	StartTime     string            `json:"start_time"`
	EndTime       string            `json:"end_time"`
	HangupCause   string            `json:"hangup_cause"`
	HangupSource  string            `json:"hangup_source"`
	StreamID      string            `json:"stream_id,omitempty"`
	CustomParams  map[string]string `json:"custom_parameters,omitempty"`
}) {
	g.logger.Info("streaming stopped",
		"call_id", payload.CallControlID,
		"stream_id", payload.StreamID)
	w.WriteHeader(http.StatusOK)
}

// handleCallStatus handles Telnyx call control webhooks (alternative endpoint).
func (g *Gateway) handleCallStatus(w http.ResponseWriter, r *http.Request) {
	// This endpoint can be used for status-only callbacks
	// For full event handling, use handleInboundCall
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)

	var event webhookEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		g.logger.Error("failed to parse status webhook", "error", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	g.logger.Info("call status update",
		"event_type", event.Data.EventType,
		"call_id", sanitize(event.Data.Payload.CallControlID),
		"state", sanitize(event.Data.Payload.State))

	// Update call system
	g.callSystem.HandleCallEvent(event.Data.Payload.CallControlID, event.Data.EventType)

	w.WriteHeader(http.StatusOK)
}

// handleMediaStream handles the WebSocket connection for Media Streaming.
func (g *Gateway) handleMediaStream(w http.ResponseWriter, r *http.Request) {
	// Upgrade to WebSocket
	wsConn, err := g.upgrader.Upgrade(w, r, nil)
	if err != nil {
		g.logger.Error("websocket upgrade failed", "error", err)
		return
	}

	g.logger.Info("media stream websocket connected",
		"remote_addr", r.RemoteAddr)

	// Create connection wrapper
	// Audio buffers sized for ~10 seconds of 8kHz μ-law audio (500 chunks * 20ms)
	conn := &mediaStreamConn{
		wsConn:        wsConn,
		gateway:       g,
		events:        make(chan Event, 100),
		audioIn:       make(chan []byte, 250), // ~5 sec input buffer
		audioOut:      make(chan []byte, 500), // ~10 sec output buffer
		done:          make(chan struct{}),
		sessionReady:  make(chan struct{}),
		pipelineReady: make(chan struct{}),
	}

	// Start read/write loops
	go conn.readLoop()
	go conn.writeLoop()

	// Wait for session to be associated (from "start" message)
	select {
	case <-conn.done:
		return
	case <-time.After(30 * time.Second):
		g.logger.Warn("no start message received, closing connection")
		_ = wsConn.Close()
		return
	case <-conn.sessionReady:
		// Session is ready, start the pipeline
	}

	// Get the session
	if conn.session == nil {
		g.logger.Error("session not set after start message")
		_ = wsConn.Close()
		return
	}

	// Start the voice pipeline
	conn.session.startPipeline(conn)

	// Wait for connection to close
	<-conn.done
}

// sanitize removes newlines and carriage returns to prevent log injection.
func sanitize(s string) string {
	s = strings.ReplaceAll(s, "\n", "")
	s = strings.ReplaceAll(s, "\r", "")
	return s
}
