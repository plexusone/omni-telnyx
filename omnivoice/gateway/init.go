package gateway

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"time"

	"github.com/plexusone/omnillm-core/provider"
	omnivoice "github.com/plexusone/omnivoice-core"
	coregateway "github.com/plexusone/omnivoice-core/gateway"
	"github.com/plexusone/omnivoice-core/registry"
)

func init() {
	omnivoice.RegisterGatewayProvider("telnyx", NewGatewayProvider, omnivoice.PriorityThick)
}

// NewGatewayProvider creates a Telnyx gateway from registry config.
func NewGatewayProvider(cfg registry.ProviderConfig) (registry.Gateway, error) {
	config := Config{
		APIKey: cfg.APIKey,
	}

	// Apply optional configuration
	if v := getExtString(cfg.Extensions, "phoneNumber"); v != "" {
		config.PhoneNumber = v
	}
	if v := getExtString(cfg.Extensions, "connectionID"); v != "" {
		config.ConnectionID = v
	}
	if v := getExtString(cfg.Extensions, "listenAddr"); v != "" {
		config.ListenAddr = v
	}
	if v := getExtString(cfg.Extensions, "publicURL"); v != "" {
		config.PublicURL = v
	}
	if v, ok := cfg.Extensions["listener"].(net.Listener); ok {
		config.Listener = v
	}

	// STT configuration
	if v := getExtString(cfg.Extensions, "sttProvider"); v != "" {
		config.STTProvider = v
	}
	if v := getExtString(cfg.Extensions, "sttAPIKey"); v != "" {
		config.STTAPIKey = v
	}
	if v := getExtString(cfg.Extensions, "sttModel"); v != "" {
		config.STTModel = v
	}
	if v := getExtString(cfg.Extensions, "sttLanguage"); v != "" {
		config.STTLanguage = v
	}

	// TTS configuration
	if v := getExtString(cfg.Extensions, "ttsProvider"); v != "" {
		config.TTSProvider = v
	}
	if v := getExtString(cfg.Extensions, "ttsAPIKey"); v != "" {
		config.TTSAPIKey = v
	}
	if v := getExtString(cfg.Extensions, "ttsVoiceID"); v != "" {
		config.TTSVoiceID = v
	}
	if v := getExtString(cfg.Extensions, "ttsModel"); v != "" {
		config.TTSModel = v
	}

	// Pipeline mode
	if v := getExtString(cfg.Extensions, "mode"); v != "" {
		config.Mode = coregateway.PipelineMode(v)
	}

	// LLM configuration
	if v := getExtString(cfg.Extensions, "llmProvider"); v != "" {
		config.LLMProvider = v
	}
	if v := getExtString(cfg.Extensions, "llmAPIKey"); v != "" {
		config.LLMAPIKey = v
	}
	if v := getExtString(cfg.Extensions, "llmModel"); v != "" {
		config.LLMModel = v
	}
	if v := getExtString(cfg.Extensions, "llmSystemPrompt"); v != "" {
		config.LLMSystemPrompt = v
	}

	// LLM client injection (type-safe)
	if v, ok := cfg.Extensions["llmClient"].(provider.Provider); ok {
		config.LLMClient = v
	}

	// Realtime configuration (type-safe)
	if v, ok := cfg.Extensions["realtimeProviderFactory"].(coregateway.RealtimeProviderFactory); ok {
		config.RealtimeProvider = v
	}
	if v, ok := cfg.Extensions["realtimeConfig"].(*coregateway.RealtimeConfig); ok {
		config.RealtimeConfig = v
	}

	// Tools configuration (type-safe)
	if v, ok := cfg.Extensions["tools"].([]ToolDefinition); ok {
		config.Tools = v
	}
	if v, ok := cfg.Extensions["toolHandlers"].(map[string]ToolHandler); ok {
		config.ToolHandlers = v
	}

	// Session configuration
	if v := getExtString(cfg.Extensions, "greeting"); v != "" {
		config.Greeting = v
	}
	if v, ok := cfg.Extensions["maxSessionDuration"].(time.Duration); ok {
		config.MaxSessionDuration = v
	}
	if v := getExtString(cfg.Extensions, "interruptionMode"); v != "" {
		config.InterruptionMode = v
	}
	if v, ok := cfg.Extensions["logger"].(*slog.Logger); ok {
		config.Logger = v
	}

	// Validate required fields
	if config.APIKey == "" {
		return nil, fmt.Errorf("telnyx gateway: apiKey is required")
	}

	gw, err := New(config)
	if err != nil {
		return nil, err
	}
	return &gatewayWrapper{gw}, nil
}

// gatewayWrapper wraps Gateway to implement registry.Gateway interface.
type gatewayWrapper struct {
	gw *Gateway
}

func (w *gatewayWrapper) Name() string {
	return string(w.gw.Name())
}

func (w *gatewayWrapper) Start(ctx any) error {
	if c, ok := ctx.(context.Context); ok {
		return w.gw.Start(c)
	}
	return w.gw.Start(context.Background())
}

func (w *gatewayWrapper) Stop() error {
	return w.gw.Stop()
}

// Gateway returns the underlying Telnyx Gateway for full API access.
func (w *gatewayWrapper) Gateway() *Gateway {
	return w.gw
}

func getExtString(ext map[string]any, key string) string {
	if ext == nil {
		return ""
	}
	if v, ok := ext[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
