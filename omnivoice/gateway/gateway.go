// Package gateway provides an HTTP/WebSocket gateway for handling Telnyx voice calls
// with full-duplex bidirectional audio using Telnyx Media Streaming.
//
// Architecture:
//
//	┌──────────┐        ┌─────────────────┐        ┌───────────────────┐
//	│  Caller  │◄──────►│     Telnyx      │◄──────►│   OmniVoice       │
//	│  (PSTN)  │  PSTN  │ Media Streaming │   WS   │   Voice Gateway   │
//	└──────────┘        └─────────────────┘        └───────────────────┘
//
// Flow:
//  1. Caller dials your Telnyx phone number
//  2. Telnyx webhook hits /voice/inbound with JSON payload
//  3. Server answers via Call Control API and starts media streaming
//  4. Telnyx opens WebSocket for bidirectional audio
//  5. Gateway receives audio, processes with STT → LLM → TTS
//  6. Gateway sends audio back through the same WebSocket
package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	coregateway "github.com/plexusone/omnivoice-core/gateway"

	"github.com/plexusone/omni-telnyx/omnivoice/callsystem"
)

// Verify interface compliance at compile time.
var _ coregateway.Gateway = (*Gateway)(nil)

// Type aliases for core gateway types.
type (
	CallHandler = coregateway.CallHandler
	CallInfo    = coregateway.CallInfo
	Turn        = coregateway.Turn
	ToolCall    = coregateway.ToolCall
	Metrics     = coregateway.Metrics
	Event       = coregateway.Event
	EventType   = coregateway.EventType
)

// Event type constants.
const (
	EventSessionStarted   = coregateway.EventSessionStarted
	EventSessionEnded     = coregateway.EventSessionEnded
	EventUserSpeechStart  = coregateway.EventUserSpeechStart
	EventUserSpeechEnd    = coregateway.EventUserSpeechEnd
	EventUserTranscript   = coregateway.EventUserTranscript
	EventAgentThinking    = coregateway.EventAgentThinking
	EventAgentSpeechStart = coregateway.EventAgentSpeechStart
	EventAgentSpeechEnd   = coregateway.EventAgentSpeechEnd
	EventAgentTranscript  = coregateway.EventAgentTranscript
	EventToolCall         = coregateway.EventToolCall
	EventInterruption     = coregateway.EventInterruption
	EventError            = coregateway.EventError
)

// Config configures the voice gateway.
type Config struct {
	// Telnyx credentials
	APIKey       string
	PhoneNumber  string
	ConnectionID string

	// Server configuration
	ListenAddr string       // e.g., ":8080"
	PublicURL  string       // e.g., "https://your-server.com"
	Listener   net.Listener // Optional external listener (e.g., ngrok)

	// Voice pipeline configuration
	STTProvider string // e.g., "deepgram", "whisper"
	STTAPIKey   string
	STTModel    string
	STTLanguage string

	TTSProvider string // e.g., "elevenlabs", "openai"
	TTSAPIKey   string
	TTSVoiceID  string
	TTSModel    string

	LLMProvider     string // e.g., "anthropic", "openai"
	LLMAPIKey       string
	LLMModel        string // e.g., "claude-sonnet-4-20250514"
	LLMSystemPrompt string

	// Tools available to the LLM
	Tools        []ToolDefinition
	ToolHandlers map[string]ToolHandler

	// Greeting is the initial message spoken when a call connects.
	Greeting string

	// Session configuration
	MaxSessionDuration time.Duration
	InterruptionMode   string // "immediate", "after_sentence", "disabled"

	// Logging
	Logger *slog.Logger
}

// Gateway handles Telnyx voice calls with full-duplex audio.
type Gateway struct {
	config      Config
	callSystem  *callsystem.Provider
	logger      *slog.Logger
	upgrader    websocket.Upgrader
	callHandler CallHandler

	mu       sync.RWMutex
	sessions map[string]*Session
	server   *http.Server
}

// New creates a new Telnyx voice gateway.
func New(cfg Config) (*Gateway, error) {
	if cfg.APIKey == "" {
		return nil, fmt.Errorf("APIKey is required")
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":8080"
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.MaxSessionDuration == 0 {
		cfg.MaxSessionDuration = 30 * time.Minute
	}
	if cfg.InterruptionMode == "" {
		cfg.InterruptionMode = "immediate"
	}

	// Create call system
	cs, err := callsystem.New(
		callsystem.WithAPIKey(cfg.APIKey),
		callsystem.WithPhoneNumber(cfg.PhoneNumber),
		callsystem.WithConnectionID(cfg.ConnectionID),
		callsystem.WithWebhookURL(cfg.PublicURL+"/ws/media-stream"),
	)
	if err != nil {
		return nil, fmt.Errorf("create call system: %w", err)
	}

	return &Gateway{
		config:     cfg,
		callSystem: cs,
		logger:     cfg.Logger,
		upgrader: websocket.Upgrader{
			CheckOrigin:     func(r *http.Request) bool { return true },
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
		},
		sessions: make(map[string]*Session),
	}, nil
}

// Name returns the provider name.
func (g *Gateway) Name() coregateway.ProviderName {
	return coregateway.ProviderTelnyx
}

// OnCall sets the handler for incoming calls.
func (g *Gateway) OnCall(handler CallHandler) {
	g.mu.Lock()
	g.callHandler = handler
	g.mu.Unlock()
}

// Start starts the gateway server.
func (g *Gateway) Start(ctx context.Context) error {
	mux := http.NewServeMux()

	// Telnyx webhook handlers
	mux.HandleFunc("/voice/inbound", g.handleInboundCall)
	mux.HandleFunc("/voice/status", g.handleCallStatus)

	// WebSocket handler for Media Streaming
	mux.HandleFunc("/ws/media-stream", g.handleMediaStream)

	// Health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	g.server = &http.Server{
		Addr:              g.config.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	g.logger.Info("starting Telnyx voice gateway",
		"addr", g.config.ListenAddr,
		"public_url", g.config.PublicURL,
		"external_listener", g.config.Listener != nil)

	errCh := make(chan error, 1)
	go func() {
		var err error
		if g.config.Listener != nil {
			// Use external listener (e.g., ngrok)
			err = g.server.Serve(g.config.Listener)
		} else {
			// Create our own listener
			err = g.server.ListenAndServe()
		}
		if err != http.ErrServerClosed {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		return g.Stop()
	case err := <-errCh:
		return err
	}
}

// Stop gracefully shuts down the gateway.
func (g *Gateway) Stop() error {
	g.logger.Info("stopping Telnyx voice gateway")

	// Close all active sessions
	g.mu.Lock()
	for _, session := range g.sessions {
		_ = session.Close()
	}
	g.sessions = make(map[string]*Session)
	g.mu.Unlock()

	// Shutdown server
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if g.server != nil {
		return g.server.Shutdown(ctx)
	}
	return nil
}

// MakeCall initiates an outbound call.
func (g *Gateway) MakeCall(ctx context.Context, to string) (coregateway.Session, error) {
	call, err := g.callSystem.MakeCall(ctx, to)
	if err != nil {
		return nil, fmt.Errorf("make call: %w", err)
	}

	session := g.createSession(call.ID(), call.From(), to, "outbound")
	return session, nil
}

// GetSession retrieves an active session by call ID.
func (g *Gateway) GetSession(callID string) (coregateway.Session, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	session, ok := g.sessions[callID]
	if !ok {
		return nil, false
	}
	return session, true
}

// ListSessions returns all active sessions.
func (g *Gateway) ListSessions() []coregateway.Session {
	g.mu.RLock()
	defer g.mu.RUnlock()

	sessions := make([]coregateway.Session, 0, len(g.sessions))
	for _, s := range g.sessions {
		sessions = append(sessions, s)
	}
	return sessions
}

// getSessionInternal retrieves the concrete session type (for internal use).
func (g *Gateway) getSessionInternal(callID string) (*Session, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	session, ok := g.sessions[callID]
	return session, ok
}

// createSession creates a new voice session.
func (g *Gateway) createSession(callID, from, to, direction string) *Session {
	session := &Session{
		id:        callID,
		gateway:   g,
		from:      from,
		to:        to,
		direction: direction,
		startTime: time.Now(),
		events:    make(chan coregateway.Event, 100),
		done:      make(chan struct{}),
		logger:    g.logger.With("call_id", callID),
	}

	g.mu.Lock()
	g.sessions[callID] = session
	g.mu.Unlock()

	return session
}

// removeSession removes a session from the gateway.
func (g *Gateway) removeSession(callID string) {
	g.mu.Lock()
	delete(g.sessions, callID)
	g.mu.Unlock()
}

// CallSystem returns the underlying call system provider.
func (g *Gateway) CallSystem() *callsystem.Provider {
	return g.callSystem
}
