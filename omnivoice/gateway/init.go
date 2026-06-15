package gateway

import (
	"context"
	"fmt"
	"net"

	omnivoice "github.com/plexusone/omnivoice-core"
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
	if v, ok := cfg.Extensions["listener"]; ok {
		if listener, ok := v.(net.Listener); ok {
			config.Listener = listener
		}
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

	// LLM configuration
	if v := getExtString(cfg.Extensions, "llmProvider"); v != "" {
		config.LLMProvider = v
	}
	if v := getExtString(cfg.Extensions, "llmModel"); v != "" {
		config.LLMModel = v
	}
	if v := getExtString(cfg.Extensions, "llmSystemPrompt"); v != "" {
		config.LLMSystemPrompt = v
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
