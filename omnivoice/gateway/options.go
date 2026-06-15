package gateway

import (
	"github.com/plexusone/omnillm-core/provider"
	coregateway "github.com/plexusone/omnivoice-core/gateway"
	"github.com/plexusone/omnivoice-core/registry"
)

// Provider-specific option functions for type-safe configuration via the registry.
// These functions return registry.ProviderOption and can be used with
// omnivoice.GetGatewayProvider("telnyx", opts...).

// WithTools sets the tools available to the LLM.
func WithTools(tools []ToolDefinition) registry.ProviderOption {
	return registry.WithExtension("tools", tools)
}

// WithToolHandlers sets the tool handlers.
func WithToolHandlers(handlers map[string]ToolHandler) registry.ProviderOption {
	return registry.WithExtension("toolHandlers", handlers)
}

// WithToolHandler adds a single tool handler.
func WithToolHandler(name string, handler ToolHandler) registry.ProviderOption {
	return func(cfg *registry.ProviderConfig) {
		if cfg.Extensions == nil {
			cfg.Extensions = make(map[string]any)
		}
		handlers, ok := cfg.Extensions["toolHandlers"].(map[string]ToolHandler)
		if !ok || handlers == nil {
			handlers = make(map[string]ToolHandler)
		}
		handlers[name] = handler
		cfg.Extensions["toolHandlers"] = handlers
	}
}

// WithLLMClient sets a pre-configured LLM provider.
// This allows injecting thick providers (official SDKs) from the application.
func WithLLMClient(client provider.Provider) registry.ProviderOption {
	return registry.WithExtension("llmClient", client)
}

// WithRealtimeProviderFactory sets the realtime provider factory for voice-to-voice mode.
func WithRealtimeProviderFactory(factory coregateway.RealtimeProviderFactory) registry.ProviderOption {
	return registry.WithExtension("realtimeProviderFactory", factory)
}

// WithRealtimeConfig sets the realtime configuration for voice-to-voice mode.
func WithRealtimeConfig(config *coregateway.RealtimeConfig) registry.ProviderOption {
	return registry.WithExtension("realtimeConfig", config)
}
