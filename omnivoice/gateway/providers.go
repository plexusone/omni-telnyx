package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	omnillm "github.com/plexusone/omnillm-core"
	"github.com/plexusone/omnillm-core/provider"
	omnivoice "github.com/plexusone/omnivoice-core"
	"github.com/plexusone/omnivoice-core/audio/codec"
	"github.com/plexusone/omnivoice-core/registry"
	"github.com/plexusone/omnivoice-core/stt"
	"github.com/plexusone/omnivoice-core/tts"
)

// defaultSTTProvider wraps an omnivoice STT provider.
type defaultSTTProvider struct {
	provider     stt.Provider
	providerName string
	config       stt.TranscriptionConfig
}

func createSTTProvider(cfg Config) (STTProvider, error) {
	providerName := cfg.STTProvider
	if providerName == "" {
		providerName = "openai" // Default to OpenAI Whisper
	}

	// Normalize provider name
	if providerName == "whisper" {
		providerName = "openai"
	}

	apiKey := cfg.STTAPIKey
	if apiKey == "" {
		// Try environment variables
		switch providerName {
		case "deepgram":
			apiKey = os.Getenv("DEEPGRAM_API_KEY")
		case "openai":
			apiKey = os.Getenv("OPENAI_API_KEY")
		case "elevenlabs":
			apiKey = os.Getenv("ELEVENLABS_API_KEY")
		case "google":
			apiKey = os.Getenv("GOOGLE_API_KEY")
		}
	}

	if apiKey == "" {
		return nil, fmt.Errorf("STT provider %s requires API key (set %s_API_KEY)", providerName, strings.ToUpper(providerName))
	}

	// Use omnivoice-core registry to get the provider
	// Note: Application must import the desired provider packages (e.g., omni-deepgram, omni-openai)
	// to register them before calling this function
	provider, err := omnivoice.GetSTTProvider(providerName, registry.WithAPIKey(apiKey))
	if err != nil {
		return nil, fmt.Errorf("create STT provider: %w", err)
	}

	// Configure encoding based on provider
	// Deepgram supports native μ-law input; OpenAI requires WAV
	encoding := "mulaw"
	if providerName == "openai" {
		encoding = "wav" // OpenAI Whisper needs WAV container
	}

	return &defaultSTTProvider{
		provider:     provider,
		providerName: providerName,
		config: stt.TranscriptionConfig{
			Model:      cfg.STTModel,
			Language:   cfg.STTLanguage,
			Encoding:   encoding,
			SampleRate: 8000, // Telnyx uses 8kHz
			Channels:   1,    // Mono audio
		},
	}, nil
}

func (p *defaultSTTProvider) Transcribe(ctx context.Context, audio []byte) (string, error) {
	if p.provider == nil {
		return "", fmt.Errorf("STT provider not initialized")
	}

	// Prepare audio based on provider requirements
	// Telnyx sends raw μ-law 8kHz audio
	var inputAudio []byte
	switch p.providerName {
	case "deepgram":
		// Deepgram accepts raw μ-law directly - no conversion needed
		inputAudio = audio
	case "openai":
		// OpenAI Whisper requires a proper audio file format (WAV, MP3, etc.)
		inputAudio = codec.MulawToWAV(audio)
	default:
		// For unknown providers, try WAV as it's widely supported
		inputAudio = codec.MulawToWAV(audio)
	}

	result, err := p.provider.Transcribe(ctx, inputAudio, p.config)
	if err != nil {
		return "", err
	}
	return result.Text, nil
}

// defaultLLMProvider wraps an omnillm provider.
type defaultLLMProvider struct {
	provider     provider.Provider
	model        string
	systemPrompt string
	tools        []provider.Tool
	toolHandlers map[string]ToolHandler
}

func createLLMProvider(cfg Config) (LLMProvider, error) {
	var llmProvider provider.Provider

	// Use injected provider if available
	if cfg.LLMClient != nil {
		llmProvider = cfg.LLMClient
	} else {
		// Fall back to omnillm-core registry (thin providers)
		providerName := cfg.LLMProvider
		if providerName == "" {
			providerName = "openai" // Default
		}

		apiKey := cfg.LLMAPIKey
		if apiKey == "" {
			// Try environment variables
			switch providerName {
			case "anthropic":
				apiKey = os.Getenv("ANTHROPIC_API_KEY")
			case "openai":
				apiKey = os.Getenv("OPENAI_API_KEY")
			}
		}

		if apiKey == "" {
			return nil, fmt.Errorf("LLM provider %s requires API key (set %s_API_KEY)", providerName, strings.ToUpper(providerName))
		}

		// Get provider factory from omnillm-core registry
		factory := omnillm.GetProviderFactory(omnillm.ProviderName(providerName))
		if factory == nil {
			return nil, fmt.Errorf("LLM provider %s not registered", providerName)
		}

		var err error
		llmProvider, err = factory(omnillm.ProviderConfig{
			Provider: omnillm.ProviderName(providerName),
			APIKey:   apiKey,
		})
		if err != nil {
			return nil, fmt.Errorf("create LLM provider: %w", err)
		}
	}

	model := cfg.LLMModel
	if model == "" {
		providerName := cfg.LLMProvider
		if providerName == "" {
			providerName = "openai"
		}
		switch providerName {
		case "anthropic":
			model = "claude-sonnet-4-20250514"
		case "openai":
			model = "gpt-4o"
		}
	}

	systemPrompt := cfg.LLMSystemPrompt
	if systemPrompt == "" {
		systemPrompt = defaultSystemPrompt
	}

	// Convert ToolDefinitions to provider.Tool format
	var tools []provider.Tool
	for _, td := range cfg.Tools {
		tools = append(tools, provider.Tool{
			Type: "function",
			Function: provider.ToolSpec{
				Name:        td.Name,
				Description: td.Description,
				Parameters:  td.Parameters,
			},
		})
	}

	return &defaultLLMProvider{
		provider:     llmProvider,
		model:        model,
		systemPrompt: systemPrompt,
		tools:        tools,
		toolHandlers: cfg.ToolHandlers,
	}, nil
}

func (p *defaultLLMProvider) Generate(ctx context.Context, input string, history []Turn) (string, []ToolCall, error) {
	// Build messages
	messages := make([]provider.Message, 0, len(history)+2)

	// Add system prompt
	if p.systemPrompt != "" {
		messages = append(messages, provider.Message{
			Role:    provider.RoleSystem,
			Content: p.systemPrompt,
		})
	}

	// Add history
	for _, turn := range history {
		role := provider.RoleUser
		if turn.Role == "agent" {
			role = provider.RoleAssistant
		}
		messages = append(messages, provider.Message{
			Role:    role,
			Content: turn.Text,
		})
	}

	// Add current input
	messages = append(messages, provider.Message{
		Role:    provider.RoleUser,
		Content: input,
	})

	// Build request with tools if available
	req := &provider.ChatCompletionRequest{
		Model:    p.model,
		Messages: messages,
	}
	if len(p.tools) > 0 {
		req.Tools = p.tools
	}

	// Tool call loop - keep calling until we get a final response
	const maxToolIterations = 10
	for i := 0; i < maxToolIterations; i++ {
		resp, err := p.provider.CreateChatCompletion(ctx, req)
		if err != nil {
			return "", nil, fmt.Errorf("LLM completion: %w", err)
		}

		if len(resp.Choices) == 0 {
			return "", nil, fmt.Errorf("no response from LLM")
		}

		choice := resp.Choices[0]

		// Check if we have tool calls to process
		if len(choice.Message.ToolCalls) > 0 && p.toolHandlers != nil {
			// Add assistant message with tool calls
			messages = append(messages, choice.Message)

			// Execute each tool call
			var toolCalls []ToolCall
			for _, tc := range choice.Message.ToolCalls {
				// Parse arguments
				var args map[string]any
				if err := json.Unmarshal([]byte(tc.Function.Arguments), &args); err != nil {
					args = make(map[string]any)
				}

				toolCalls = append(toolCalls, ToolCall{
					Name:      tc.Function.Name,
					Arguments: args,
				})

				// Execute tool handler if available
				var result string
				if handler, ok := p.toolHandlers[tc.Function.Name]; ok {
					toolResult, toolErr := handler(ctx, args)
					if toolErr != nil {
						result = fmt.Sprintf("Error: %v", toolErr)
					} else {
						result = toolResult
					}
				} else {
					result = fmt.Sprintf("Tool %s not found", tc.Function.Name)
				}

				// Add tool result message
				toolCallID := tc.ID
				messages = append(messages, provider.Message{
					Role:       provider.RoleTool,
					Content:    result,
					ToolCallID: &toolCallID,
				})
			}

			// Update request with new messages for next iteration
			req.Messages = messages
			continue
		}

		// No tool calls, return the final response
		return choice.Message.Content, nil, nil
	}

	return "", nil, fmt.Errorf("exceeded maximum tool iterations")
}

// defaultTTSProvider wraps an omnivoice TTS provider.
type defaultTTSProvider struct {
	provider     tts.Provider
	config       tts.SynthesisConfig
	providerName string
}

func createTTSProvider(cfg Config) (TTSProvider, error) {
	providerName := cfg.TTSProvider
	if providerName == "" {
		providerName = "openai" // Default to OpenAI TTS
	}

	apiKey := cfg.TTSAPIKey
	if apiKey == "" {
		// Try environment variables
		switch providerName {
		case "elevenlabs":
			apiKey = os.Getenv("ELEVENLABS_API_KEY")
		case "openai":
			apiKey = os.Getenv("OPENAI_API_KEY")
		case "deepgram":
			apiKey = os.Getenv("DEEPGRAM_API_KEY")
		case "google":
			apiKey = os.Getenv("GOOGLE_API_KEY")
		}
	}

	if apiKey == "" {
		return nil, fmt.Errorf("TTS provider %s requires API key (set %s_API_KEY)", providerName, strings.ToUpper(providerName))
	}

	voiceID := cfg.TTSVoiceID
	if voiceID == "" {
		switch providerName {
		case "elevenlabs":
			voiceID = "21m00Tcm4TlvDq8ikWAM" // Rachel
		case "openai":
			voiceID = "alloy"
		case "deepgram":
			voiceID = "aura-asteria-en"
		}
	}

	// Use omnivoice-core registry to get the provider
	// Note: Application must import the desired provider packages (e.g., omni-deepgram, omni-openai)
	// to register them before calling this function
	provider, err := omnivoice.GetTTSProvider(providerName, registry.WithAPIKey(apiKey))
	if err != nil {
		return nil, fmt.Errorf("create TTS provider: %w", err)
	}

	// Configure output format based on provider
	var outputFormat string
	var sampleRate int
	switch providerName {
	case "openai":
		// OpenAI TTS supports PCM output at 24kHz
		outputFormat = "pcm"
		sampleRate = 24000
	case "elevenlabs":
		// ElevenLabs supports various formats - use ulaw for direct Telnyx compatibility
		outputFormat = "ulaw_8000"
		sampleRate = 8000
	case "deepgram":
		// Deepgram Aura supports mulaw
		outputFormat = "mulaw"
		sampleRate = 8000
	default:
		outputFormat = "mp3"
		sampleRate = 44100
	}

	return &defaultTTSProvider{
		provider:     provider,
		providerName: providerName,
		config: tts.SynthesisConfig{
			VoiceID:      voiceID,
			Model:        cfg.TTSModel,
			OutputFormat: outputFormat,
			SampleRate:   sampleRate,
		},
	}, nil
}

func (p *defaultTTSProvider) Synthesize(ctx context.Context, text string) ([]byte, error) {
	if p.provider == nil {
		return nil, fmt.Errorf("TTS provider not initialized")
	}

	result, err := p.provider.Synthesize(ctx, text, p.config)
	if err != nil {
		return nil, err
	}

	// Convert audio to μ-law 8kHz for Telnyx
	switch p.providerName {
	case "openai":
		// OpenAI outputs PCM 24kHz 16-bit - convert to μ-law 8kHz
		pcm16 := codec.BytesToInt16(result.Audio, false)
		resampled := codec.Resample(pcm16, codec.SampleRate(24000), codec.SampleRate8kHz)
		return codec.MulawEncode(resampled), nil

	case "elevenlabs", "deepgram":
		// If provider outputs μ-law or compatible format, return as-is
		// The config requests ulaw_8000 or mulaw format
		return result.Audio, nil

	default:
		// For other providers, assume PCM and convert
		if result.Format == "pcm" || result.Format == "linear16" {
			pcm16 := codec.BytesToInt16(result.Audio, false)
			fromRate := codec.SampleRate(result.SampleRate)
			if fromRate == 0 {
				fromRate = codec.SampleRate44100Hz
			}
			resampled := codec.Resample(pcm16, fromRate, codec.SampleRate8kHz)
			return codec.MulawEncode(resampled), nil
		}
		// Return raw audio for unsupported formats
		return result.Audio, nil
	}
}

// Default system prompt for the voice agent.
const defaultSystemPrompt = `You are a helpful voice assistant. You are having a phone conversation with a user.

Guidelines:
- Keep responses concise and conversational
- Use natural spoken language, not written language
- Avoid using markdown, bullet points, or formatting
- If you need to list things, say them naturally
- Ask clarifying questions when needed
- Be friendly and professional

Remember: This is a voice conversation, so speak naturally as you would on the phone.`

// ToolDefinition defines a tool that the LLM can call.
type ToolDefinition struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
}

// ToolHandler processes a tool call.
type ToolHandler func(ctx context.Context, args map[string]any) (string, error)

// AgentConfig extends the gateway config with agent-specific settings.
type AgentConfig struct {
	// Tools available to the agent
	Tools []ToolDefinition

	// Tool handlers
	Handlers map[string]ToolHandler

	// Greeting message (spoken when call connects)
	Greeting string

	// Goodbye message (spoken before hanging up)
	Goodbye string

	// Transfer number (for transferring calls)
	TransferNumber string
}

// FormatToolResult formats a tool result for the LLM.
func FormatToolResult(name string, result any, err error) string {
	if err != nil {
		return fmt.Sprintf("Tool %s failed: %v", name, err)
	}

	switch v := result.(type) {
	case string:
		return v
	case nil:
		return "Success"
	default:
		data, _ := json.Marshal(v)
		return string(data)
	}
}

// ParseToolCall parses a tool call from LLM output.
func ParseToolCall(output string) (name string, args map[string]any, found bool) {
	// Look for tool call markers in the output
	// This is a simplified parser - production would use structured output

	if !strings.Contains(output, "<tool_call>") {
		return "", nil, false
	}

	// Extract tool call
	start := strings.Index(output, "<tool_call>")
	end := strings.Index(output, "</tool_call>")
	if start == -1 || end == -1 || end <= start {
		return "", nil, false
	}

	toolCall := output[start+len("<tool_call>") : end]

	// Parse JSON
	var call struct {
		Name string         `json:"name"`
		Args map[string]any `json:"arguments"`
	}
	if err := json.Unmarshal([]byte(toolCall), &call); err != nil {
		return "", nil, false
	}

	return call.Name, call.Args, true
}
