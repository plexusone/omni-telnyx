package gateway

import (
	"context"
	"log/slog"
	"sync"
	"time"

	coregateway "github.com/plexusone/omnivoice-core/gateway"
)

// Verify interface compliance at compile time.
var _ coregateway.Session = (*Session)(nil)

// Session represents an active voice conversation session.
type Session struct {
	id        string
	gateway   *Gateway
	from      string
	to        string
	direction string
	startTime time.Time
	events    chan coregateway.Event
	done      chan struct{}
	logger    *slog.Logger

	mu         sync.RWMutex
	conn       *mediaStreamConn
	pipeline   *Pipeline
	transcript []coregateway.Turn
	metrics    coregateway.Metrics
	closed     bool
	closeOnce  sync.Once
}

// ID returns the session identifier (call control ID).
func (s *Session) ID() string {
	return s.id
}

// From returns the caller phone number.
func (s *Session) From() string {
	return s.from
}

// To returns the called phone number.
func (s *Session) To() string {
	return s.to
}

// Direction returns "inbound" or "outbound".
func (s *Session) Direction() string {
	return s.direction
}

// StartTime returns when the session started.
func (s *Session) StartTime() time.Time {
	return s.startTime
}

// Duration returns the session duration.
func (s *Session) Duration() time.Duration {
	return time.Since(s.startTime)
}

// Events returns a channel for session events.
func (s *Session) Events() <-chan coregateway.Event {
	return s.events
}

// Transcript returns the conversation transcript.
func (s *Session) Transcript() []coregateway.Turn {
	s.mu.RLock()
	defer s.mu.RUnlock()

	transcript := make([]coregateway.Turn, len(s.transcript))
	copy(transcript, s.transcript)
	return transcript
}

// Metrics returns session performance metrics.
func (s *Session) Metrics() coregateway.Metrics {
	s.mu.RLock()
	defer s.mu.RUnlock()

	metrics := s.metrics
	metrics.SessionDurationMs = int(time.Since(s.startTime).Milliseconds())
	return metrics
}

// SendText sends text input to the agent (bypasses STT).
func (s *Session) SendText(text string) error {
	s.mu.RLock()
	pipeline := s.pipeline
	s.mu.RUnlock()

	if pipeline == nil {
		return nil
	}

	return pipeline.ProcessText(context.Background(), text)
}

// Interrupt stops the current agent speech.
func (s *Session) Interrupt() {
	s.mu.RLock()
	pipeline := s.pipeline
	s.mu.RUnlock()

	if pipeline != nil {
		pipeline.Interrupt()
	}

	s.emitEvent(EventInterruption, nil)
	s.mu.Lock()
	s.metrics.InterruptionCount++
	s.mu.Unlock()
}

// Close ends the session.
func (s *Session) Close() error {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()

		close(s.done)

		// Stop pipeline
		if s.pipeline != nil {
			s.pipeline.Stop()
		}

		// Close connection
		if s.conn != nil {
			s.conn.close()
		}

		// Remove from gateway
		s.gateway.removeSession(s.id)

		// Close events channel
		close(s.events)

		s.logger.Info("session closed",
			"duration", s.Duration().String(),
			"turns", len(s.transcript))
	})
	return nil
}

// startPipeline starts the voice processing pipeline.
func (s *Session) startPipeline(conn *mediaStreamConn) {
	s.mu.Lock()
	s.conn = conn
	s.mu.Unlock()

	// Create pipeline
	pipeline, err := NewPipeline(s)
	if err != nil {
		s.logger.Error("failed to create pipeline", "error", err)
		s.emitEvent(EventError, err)
		return
	}

	s.mu.Lock()
	s.pipeline = pipeline
	s.mu.Unlock()

	s.emitEvent(EventSessionStarted, nil)

	// Start pipeline
	ctx, cancel := context.WithTimeout(context.Background(), s.gateway.config.MaxSessionDuration)
	defer cancel()

	if err := pipeline.Start(ctx); err != nil {
		s.logger.Error("pipeline error", "error", err)
		s.emitEvent(EventError, err)
	}
}

// emitEvent sends an event to the events channel.
func (s *Session) emitEvent(eventType coregateway.EventType, data any) {
	event := coregateway.Event{
		Type:      eventType,
		Timestamp: time.Now(),
		Data:      data,
	}

	select {
	case s.events <- event:
	default:
		// Channel full, drop event
		s.logger.Warn("event channel full, dropping event", "type", eventType)
	}
}

// addTurn adds a conversation turn to the transcript.
func (s *Session) addTurn(turn coregateway.Turn) {
	s.mu.Lock()
	s.transcript = append(s.transcript, turn)
	s.metrics.TurnCount++
	s.mu.Unlock()
}

// updateMetrics updates session metrics.
func (s *Session) updateMetrics(sttLatency, llmLatency, ttsLatency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update averages (simple running average)
	n := float64(s.metrics.TurnCount)
	if n == 0 {
		n = 1
	}

	updateAvg := func(current int, newVal time.Duration) int {
		return int((float64(current)*(n-1) + float64(newVal.Milliseconds())) / n)
	}

	s.metrics.AvgSTTLatencyMs = updateAvg(s.metrics.AvgSTTLatencyMs, sttLatency)
	s.metrics.AvgLLMLatencyMs = updateAvg(s.metrics.AvgLLMLatencyMs, llmLatency)
	s.metrics.AvgTTSLatencyMs = updateAvg(s.metrics.AvgTTSLatencyMs, ttsLatency)
	s.metrics.AvgTotalLatencyMs = updateAvg(s.metrics.AvgTotalLatencyMs, sttLatency+llmLatency+ttsLatency)
}
