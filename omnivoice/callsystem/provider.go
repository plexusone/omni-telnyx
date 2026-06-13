// Package callsystem provides a Telnyx implementation of callsystem.CallSystem.
package callsystem

import (
	"context"
	"fmt"
	"sync"

	"github.com/plexusone/omnivoice-core/callsystem"
	"github.com/team-telnyx/telnyx-go/v4"
	"github.com/team-telnyx/telnyx-go/v4/option"

	"github.com/plexusone/omni-telnyx/omnivoice/transport"
)

// Verify interface compliance at compile time.
var (
	_ callsystem.CallSystem  = (*Provider)(nil)
	_ callsystem.SMSProvider = (*Provider)(nil)
)

// Provider implements callsystem.CallSystem using Telnyx.
type Provider struct {
	client       *telnyx.Client
	config       callsystem.CallSystemConfig
	handler      callsystem.CallHandler
	transport    *transport.Provider
	defaultFrom  string
	connectionID string

	mu    sync.RWMutex
	calls map[string]*Call
}

// Option configures the Provider.
type Option func(*options)

type options struct {
	apiKey       string
	phoneNumber  string
	webhookURL   string
	connectionID string
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

// WithWebhookURL sets the webhook URL for call events.
func WithWebhookURL(url string) Option {
	return func(o *options) {
		o.webhookURL = url
	}
}

// WithConnectionID sets the Telnyx Connection ID for outbound calls.
func WithConnectionID(id string) Option {
	return func(o *options) {
		o.connectionID = id
	}
}

// New creates a new Telnyx CallSystem provider.
func New(opts ...Option) (*Provider, error) {
	cfg := &options{}
	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.apiKey == "" {
		return nil, fmt.Errorf("telnyx: API key is required")
	}

	// Create Telnyx client
	client := telnyx.NewClient(option.WithAPIKey(cfg.apiKey))

	// Create transport provider for Media Streaming
	tr, err := transport.New()
	if err != nil {
		return nil, fmt.Errorf("failed to create transport: %w", err)
	}

	return &Provider{
		client:       &client,
		transport:    tr,
		defaultFrom:  cfg.phoneNumber,
		connectionID: cfg.connectionID,
		calls:        make(map[string]*Call),
		config: callsystem.CallSystemConfig{
			APIKey:      cfg.apiKey,
			PhoneNumber: cfg.phoneNumber,
			WebhookURL:  cfg.webhookURL,
		},
	}, nil
}

// Name returns the provider name.
func (p *Provider) Name() string {
	return "telnyx"
}

// Configure configures the call system.
func (p *Provider) Configure(config callsystem.CallSystemConfig) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.config = config
	if config.PhoneNumber != "" {
		p.defaultFrom = config.PhoneNumber
	}

	return nil
}

// OnIncomingCall sets the handler for incoming calls.
func (p *Provider) OnIncomingCall(handler callsystem.CallHandler) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.handler = handler
}

// MakeCall initiates an outbound call.
func (p *Provider) MakeCall(ctx context.Context, to string, opts ...callsystem.CallOption) (callsystem.Call, error) {
	// Apply options
	callOpts := &callsystem.CallOptions{}
	for _, opt := range opts {
		opt(callOpts)
	}

	from := callOpts.From
	if from == "" {
		from = p.defaultFrom
	}
	if from == "" {
		return nil, fmt.Errorf("from number is required (use WithFrom or set default phone number)")
	}

	// Build dial parameters
	params := telnyx.CallDialParams{
		From: from,
		To: telnyx.CallDialParamsToUnion{
			OfString: telnyx.String(to),
		},
	}

	// Set connection ID if configured
	if p.connectionID != "" {
		params.ConnectionID = p.connectionID
	}

	// Set webhook URL if configured
	if p.config.WebhookURL != "" {
		params.WebhookURL = telnyx.String(p.config.WebhookURL)
	}

	// Set timeout if specified
	if callOpts.Timeout > 0 {
		params.TimeoutSecs = telnyx.Int(int64(callOpts.Timeout.Seconds()))
	}

	// Make the call
	response, err := p.client.Calls.Dial(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("failed to dial: %w", err)
	}

	call := newCall(response.Data.CallControlID, from, to, callsystem.Outbound, p)

	p.mu.Lock()
	p.calls[call.id] = call
	p.mu.Unlock()

	return call, nil
}

// GetCall retrieves a call by ID.
func (p *Provider) GetCall(ctx context.Context, callID string) (callsystem.Call, error) {
	p.mu.RLock()
	call, ok := p.calls[callID]
	p.mu.RUnlock()

	if ok {
		return call, nil
	}

	return nil, fmt.Errorf("call not found: %s", callID)
}

// ListCalls lists active calls.
func (p *Provider) ListCalls(ctx context.Context) ([]callsystem.Call, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	calls := make([]callsystem.Call, 0, len(p.calls))
	for _, call := range p.calls {
		calls = append(calls, call)
	}
	return calls, nil
}

// Close shuts down the call system.
func (p *Provider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Hangup all active calls
	ctx := context.Background()
	for _, call := range p.calls {
		_ = call.Hangup(ctx)
	}

	p.calls = make(map[string]*Call)

	if p.transport != nil {
		return p.transport.Close()
	}

	return nil
}

// HandleIncomingWebhook processes a Telnyx incoming call webhook.
// This should be called from your HTTP handler when receiving call.initiated events.
func (p *Provider) HandleIncomingWebhook(callControlID, from, to string) (callsystem.Call, error) {
	call := newCall(callControlID, from, to, callsystem.Inbound, p)

	p.mu.Lock()
	p.calls[callControlID] = call
	handler := p.handler
	p.mu.Unlock()

	// Call the handler
	if handler != nil {
		if err := handler(call); err != nil {
			return nil, err
		}
	}

	return call, nil
}

// HandleCallEvent processes Telnyx call control webhook events.
// Event types include: call.initiated, call.answered, call.hangup, call.machine.detection.ended, etc.
func (p *Provider) HandleCallEvent(callControlID, eventType string) {
	p.mu.Lock()
	call, ok := p.calls[callControlID]
	if ok {
		call.handleEvent(eventType)
		if call.status == callsystem.StatusEnded {
			delete(p.calls, callControlID)
		}
	}
	p.mu.Unlock()
}

// Transport returns the transport provider for Media Streaming.
func (p *Provider) Transport() *transport.Provider {
	return p.transport
}

// Client returns the underlying Telnyx client for advanced operations.
func (p *Provider) Client() *telnyx.Client {
	return p.client
}

// SendSMS sends an SMS message using the default phone number.
func (p *Provider) SendSMS(ctx context.Context, to, body string) (*callsystem.SMSMessage, error) {
	return p.SendSMSFrom(ctx, to, p.defaultFrom, body)
}

// SendSMSFrom sends an SMS message from a specific phone number.
func (p *Provider) SendSMSFrom(ctx context.Context, to, from, body string) (*callsystem.SMSMessage, error) {
	return p.SendMessage(ctx, &SendMessageParams{
		To:   to,
		From: from,
		Body: body,
	})
}

// SendMessageParams are parameters for sending SMS, MMS, or RCS messages.
type SendMessageParams struct {
	To      string // Recipient phone number (E.164 format)
	From    string // Sender phone number (E.164 format)
	Body    string // Message body text
	Subject string // Subject for MMS messages (optional)

	// MMS fields
	MediaURLs []string // URLs of media to attach (makes this an MMS)

	// RCS fields (via Telnyx messaging profile)
	MessagingProfileID string // Messaging profile ID for RCS/number pool
}

// SendMessage sends an SMS, MMS, or RCS message.
// If MediaURLs are provided, the message becomes an MMS.
// If MessagingProfileID is provided, the message uses that profile (for RCS or number pools).
func (p *Provider) SendMessage(ctx context.Context, params *SendMessageParams) (*callsystem.SMSMessage, error) {
	from := params.From
	if from == "" {
		from = p.defaultFrom
	}
	if from == "" && params.MessagingProfileID == "" {
		return nil, fmt.Errorf("from number or messaging profile ID is required")
	}

	telnyxParams := telnyx.MessageSendParams{
		To: params.To,
	}

	// Set sender
	if from != "" {
		telnyxParams.From = telnyx.String(from)
	}
	if params.MessagingProfileID != "" {
		telnyxParams.MessagingProfileID = telnyx.String(params.MessagingProfileID)
	}

	// Set body
	if params.Body != "" {
		telnyxParams.Text = telnyx.String(params.Body)
	}

	// MMS fields
	if params.Subject != "" {
		telnyxParams.Subject = telnyx.String(params.Subject)
	}
	if len(params.MediaURLs) > 0 {
		telnyxParams.MediaURLs = params.MediaURLs
	}

	response, err := p.client.Messages.Send(ctx, telnyxParams)
	if err != nil {
		msgType := p.messageType(params)
		return nil, fmt.Errorf("failed to send %s: %w", msgType, err)
	}

	return &callsystem.SMSMessage{
		ID:     response.Data.ID,
		To:     params.To,
		From:   from,
		Body:   params.Body,
		Status: string(response.Data.Direction),
	}, nil
}

// messageType returns a human-readable message type for error messages.
func (p *Provider) messageType(params *SendMessageParams) string {
	if params.MessagingProfileID != "" {
		return "RCS"
	}
	if len(params.MediaURLs) > 0 {
		return "MMS"
	}
	return "SMS"
}

// SendMMS sends an MMS message with media attachments.
func (p *Provider) SendMMS(ctx context.Context, to, body string, mediaURLs []string) (*callsystem.SMSMessage, error) {
	return p.SendMMSFrom(ctx, to, p.defaultFrom, body, mediaURLs)
}

// SendMMSFrom sends an MMS message from a specific phone number.
func (p *Provider) SendMMSFrom(ctx context.Context, to, from, body string, mediaURLs []string) (*callsystem.SMSMessage, error) {
	return p.SendMessage(ctx, &SendMessageParams{
		To:        to,
		From:      from,
		Body:      body,
		MediaURLs: mediaURLs,
	})
}
