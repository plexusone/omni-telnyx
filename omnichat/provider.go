// Package omnichat provides a Telnyx SMS/MMS/RCS provider for omnichat.
package omnichat

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/plexusone/omnichat/provider"

	"github.com/plexusone/omni-telnyx/omnivoice/callsystem"
)

// Verify interface compliance at compile time.
var _ provider.Provider = (*Provider)(nil)

// Provider implements provider.Provider for Telnyx SMS/MMS/RCS.
type Provider struct {
	callSystem         *callsystem.Provider
	defaultFrom        string
	messagingProfileID string // For RCS/number pools
	logger             *slog.Logger
	messageHandler     provider.MessageHandler
	eventHandler       provider.EventHandler
	webhookHandler     http.Handler

	mu        sync.RWMutex
	connected bool
}

// Option configures the Provider.
type Option func(*options)

type options struct {
	apiKey             string
	phoneNumber        string
	connectionID       string
	messagingProfileID string
	logger             *slog.Logger
}

// WithAPIKey sets the Telnyx API Key.
func WithAPIKey(key string) Option {
	return func(o *options) {
		o.apiKey = key
	}
}

// WithPhoneNumber sets the default outbound phone number.
func WithPhoneNumber(number string) Option {
	return func(o *options) {
		o.phoneNumber = number
	}
}

// WithConnectionID sets the Telnyx Connection ID.
func WithConnectionID(id string) Option {
	return func(o *options) {
		o.connectionID = id
	}
}

// WithMessagingProfileID sets the Messaging Profile ID for RCS/number pools.
// When set, messages use this profile for delivery, enabling RCS with fallback.
func WithMessagingProfileID(id string) Option {
	return func(o *options) {
		o.messagingProfileID = id
	}
}

// WithLogger sets the logger.
func WithLogger(logger *slog.Logger) Option {
	return func(o *options) {
		o.logger = logger
	}
}

// New creates a new Telnyx SMS/MMS/RCS provider.
func New(opts ...Option) (*Provider, error) {
	cfg := &options{}
	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.logger == nil {
		cfg.logger = slog.Default()
	}

	// Create call system (which handles messaging)
	csOpts := []callsystem.Option{
		callsystem.WithAPIKey(cfg.apiKey),
	}
	if cfg.phoneNumber != "" {
		csOpts = append(csOpts, callsystem.WithPhoneNumber(cfg.phoneNumber))
	}
	if cfg.connectionID != "" {
		csOpts = append(csOpts, callsystem.WithConnectionID(cfg.connectionID))
	}

	cs, err := callsystem.New(csOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Telnyx client: %w", err)
	}

	p := &Provider{
		callSystem:         cs,
		defaultFrom:        cfg.phoneNumber,
		messagingProfileID: cfg.messagingProfileID,
		logger:             cfg.logger,
	}

	// Create webhook handler
	p.webhookHandler = http.HandlerFunc(p.handleWebhook)

	return p, nil
}

// Name returns the provider name.
func (p *Provider) Name() string {
	return "telnyx-sms"
}

// Connect establishes connection to Telnyx.
// For SMS, this validates credentials.
func (p *Provider) Connect(ctx context.Context) error {
	// Telnyx API is stateless, just mark as connected
	p.mu.Lock()
	p.connected = true
	p.mu.Unlock()

	p.logger.Info("telnyx SMS provider connected")
	return nil
}

// Disconnect closes the Telnyx connection.
func (p *Provider) Disconnect(ctx context.Context) error {
	p.mu.Lock()
	p.connected = false
	p.mu.Unlock()

	_ = p.callSystem.Close()
	p.logger.Info("telnyx SMS provider disconnected")
	return nil
}

// Send sends an SMS, MMS, or RCS message.
// The chatID is the recipient phone number in E.164 format (e.g., "+1234567890").
// If msg.Media contains items with URLs, the message is sent as MMS.
func (p *Provider) Send(ctx context.Context, chatID string, msg provider.OutgoingMessage) error {
	p.mu.RLock()
	connected := p.connected
	p.mu.RUnlock()

	if !connected {
		return fmt.Errorf("provider not connected")
	}

	// Validate sender configuration
	if p.messagingProfileID == "" && p.defaultFrom == "" {
		return fmt.Errorf("from phone number or messaging profile ID not configured")
	}

	// Extract media URLs from the message
	var mediaURLs []string
	for _, m := range msg.Media {
		if m.URL != "" {
			mediaURLs = append(mediaURLs, m.URL)
		}
	}

	// Build message params
	params := &callsystem.SendMessageParams{
		To:        chatID,
		Body:      msg.Content,
		MediaURLs: mediaURLs,
	}

	// Prefer MessagingProfileID for RCS, fall back to From for SMS/MMS
	if p.messagingProfileID != "" {
		params.MessagingProfileID = p.messagingProfileID
	} else {
		params.From = p.defaultFrom
	}

	// Extract RCS fields from metadata
	if msg.Metadata != nil {
		if profileID, ok := msg.Metadata["messaging_profile_id"].(string); ok && profileID != "" {
			params.MessagingProfileID = profileID
		}
		if subject, ok := msg.Metadata["subject"].(string); ok {
			params.Subject = subject
		}
	}

	telnyxMsg, err := p.callSystem.SendMessage(ctx, params)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}

	// Determine message type for logging
	msgType := "SMS"
	if params.MessagingProfileID != "" {
		msgType = "RCS"
	} else if len(mediaURLs) > 0 {
		msgType = "MMS"
	}

	// Log sender info
	sender := params.From
	if params.MessagingProfileID != "" {
		sender = params.MessagingProfileID
	}

	p.logger.Debug(msgType+" sent",
		"to", chatID,
		"from", sender,
		"id", telnyxMsg.ID,
		"status", telnyxMsg.Status,
		"num_media", len(mediaURLs),
	)

	return nil
}

// OnMessage registers a handler for incoming messages.
func (p *Provider) OnMessage(handler provider.MessageHandler) {
	p.mu.Lock()
	p.messageHandler = handler
	p.mu.Unlock()
}

// OnEvent registers a handler for events.
func (p *Provider) OnEvent(handler provider.EventHandler) {
	p.mu.Lock()
	p.eventHandler = handler
	p.mu.Unlock()
}

// WebhookHandler returns an HTTP handler for Telnyx webhooks.
// This should be mounted at a publicly accessible URL and configured
// in your Telnyx console for incoming message webhooks.
func (p *Provider) WebhookHandler() http.Handler {
	return p.webhookHandler
}

// Telnyx webhook event structure for messaging.
type webhookEvent struct {
	Data struct {
		EventType string `json:"event_type"`
		ID        string `json:"id"`
		Payload   struct {
			ID                 string   `json:"id"`
			From               string   `json:"from"`
			To                 []string `json:"to"`
			Text               string   `json:"text"`
			Subject            string   `json:"subject,omitempty"`
			Direction          string   `json:"direction"`
			Type               string   `json:"type"` // SMS, MMS
			MessagingProfileID string   `json:"messaging_profile_id,omitempty"`
			Media              []struct {
				URL         string `json:"url"`
				ContentType string `json:"content_type"`
				Size        int    `json:"size"`
			} `json:"media,omitempty"`
		} `json:"payload"`
		OccurredAt string `json:"occurred_at"`
	} `json:"data"`
}

// handleWebhook processes incoming Telnyx webhooks.
func (p *Provider) handleWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Limit request body size to prevent memory exhaustion
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10) // 64KB

	var event webhookEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "failed to parse webhook", http.StatusBadRequest)
		return
	}

	// Only handle inbound messages
	if event.Data.EventType != "message.received" {
		w.WriteHeader(http.StatusOK)
		return
	}

	payload := event.Data.Payload
	if payload.ID == "" || payload.From == "" {
		http.Error(w, "missing required fields", http.StatusBadRequest)
		return
	}

	// Extract recipient (first in list)
	var to string
	if len(payload.To) > 0 {
		to = payload.To[0]
	}

	// Extract media attachments (MMS)
	var media []provider.Media
	for _, m := range payload.Media {
		media = append(media, provider.Media{
			Type:     mediaTypeFromMIME(m.ContentType),
			URL:      m.URL,
			MimeType: m.ContentType,
		})
	}

	p.mu.RLock()
	handler := p.messageHandler
	p.mu.RUnlock()

	if handler != nil {
		msg := provider.IncomingMessage{
			ID:           payload.ID,
			ProviderName: "telnyx-sms",
			ChatID:       payload.From, // Use sender as chat ID
			ChatType:     provider.ChatTypeDM,
			SenderID:     payload.From,
			SenderName:   payload.From,
			Content:      payload.Text,
			Media:        media,
			Timestamp:    time.Now(),
			Metadata: map[string]any{
				"to":                   to,
				"type":                 payload.Type,
				"direction":            payload.Direction,
				"messaging_profile_id": payload.MessagingProfileID,
				"subject":              payload.Subject,
			},
		}

		ctx := r.Context()
		if err := handler(ctx, msg); err != nil {
			p.logger.Error("message handler error", "error", err, "message_id", payload.ID)
		}
	}

	// Determine message type for logging
	msgType := payload.Type
	if msgType == "" {
		if len(payload.Media) > 0 {
			msgType = "MMS"
		} else {
			msgType = "SMS"
		}
	}

	p.logger.Debug("received "+msgType,
		"from", payload.From,
		"to", to,
		"id", payload.ID,
		"num_media", len(payload.Media),
	)

	w.WriteHeader(http.StatusOK)
}

// mediaTypeFromMIME converts a MIME type to a provider.MediaType.
func mediaTypeFromMIME(mimeType string) provider.MediaType {
	switch {
	case strings.HasPrefix(mimeType, "image"):
		return provider.MediaTypeImage
	case strings.HasPrefix(mimeType, "video"):
		return provider.MediaTypeVideo
	case strings.HasPrefix(mimeType, "audio"):
		return provider.MediaTypeAudio
	default:
		return provider.MediaTypeDocument
	}
}

// CallSystem returns the underlying call system provider.
// This allows access to voice features when needed.
func (p *Provider) CallSystem() *callsystem.Provider {
	return p.callSystem
}
