package gateway

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/plexusone/omnivoice-core/audio/codec"
)

// Pipeline handles the STT → LLM → TTS voice processing pipeline.
type Pipeline struct {
	session *Session
	conn    *mediaStreamConn

	// Pipeline components (interfaces to be implemented by providers)
	stt STTProvider
	llm LLMProvider
	tts TTSProvider

	// Audio buffer for collecting speech
	audioBuffer *bytes.Buffer
	bufferMu    sync.Mutex

	// State
	isSpeaking     bool
	isProcessing   bool
	lastSpeechTime time.Time
	silenceTimeout time.Duration

	// Control
	ctx         context.Context
	cancel      context.CancelFunc
	done        chan struct{}
	interrupted bool
	mu          sync.RWMutex
}

// STTProvider transcribes audio to text.
type STTProvider interface {
	// Transcribe converts audio bytes to text.
	Transcribe(ctx context.Context, audio []byte) (string, error)
}

// LLMProvider generates responses.
type LLMProvider interface {
	// Generate produces a response to the given input.
	Generate(ctx context.Context, input string, history []Turn) (string, []ToolCall, error)
}

// TTSProvider synthesizes speech from text.
type TTSProvider interface {
	// Synthesize converts text to audio.
	Synthesize(ctx context.Context, text string) ([]byte, error)
}

// NewPipeline creates a new voice processing pipeline.
func NewPipeline(session *Session) (*Pipeline, error) {
	cfg := session.gateway.config

	// Create STT provider
	stt, err := createSTTProvider(cfg)
	if err != nil {
		return nil, fmt.Errorf("create STT provider: %w", err)
	}

	// Create LLM provider
	llm, err := createLLMProvider(cfg)
	if err != nil {
		return nil, fmt.Errorf("create LLM provider: %w", err)
	}

	// Create TTS provider
	tts, err := createTTSProvider(cfg)
	if err != nil {
		return nil, fmt.Errorf("create TTS provider: %w", err)
	}

	return &Pipeline{
		session:        session,
		conn:           session.conn,
		stt:            stt,
		llm:            llm,
		tts:            tts,
		audioBuffer:    new(bytes.Buffer),
		silenceTimeout: 500 * time.Millisecond,
		done:           make(chan struct{}),
	}, nil
}

// Start begins processing the voice pipeline.
func (p *Pipeline) Start(ctx context.Context) error {
	p.ctx, p.cancel = context.WithCancel(ctx)

	// Send initial greeting if configured
	if greeting := p.session.gateway.config.Greeting; greeting != "" {
		if err := p.sendGreeting(greeting); err != nil {
			p.session.logger.Error("failed to send greeting", "error", err)
		}
	}

	// Start audio processing goroutine
	go p.processAudioLoop()

	// Start silence detection goroutine
	go p.silenceDetectionLoop()

	// Wait for completion or error
	select {
	case <-p.ctx.Done():
		return p.ctx.Err()
	case <-p.done:
		return nil
	}
}

// sendGreeting synthesizes and sends the initial greeting.
func (p *Pipeline) sendGreeting(text string) error {
	p.session.logger.Info("sending greeting", "text", text)
	p.session.emitEvent(EventAgentSpeechStart, nil)
	p.session.emitEvent(EventAgentTranscript, text)

	// Synthesize greeting
	ttsStart := time.Now()
	audio, err := p.tts.Synthesize(p.ctx, text)
	ttsLatency := time.Since(ttsStart)

	if err != nil {
		p.session.logger.Error("TTS synthesis failed for greeting", "error", err, "latency_ms", ttsLatency.Milliseconds())
		p.session.emitEvent(EventError, err)
		return fmt.Errorf("synthesize greeting: %w", err)
	}

	p.session.logger.Info("TTS greeting synthesized", "audio_bytes", len(audio), "latency_ms", ttsLatency.Milliseconds())

	if len(audio) == 0 {
		p.session.logger.Warn("TTS returned empty audio for greeting")
		return fmt.Errorf("TTS returned empty audio")
	}

	// Add greeting to transcript
	p.session.addTurn(Turn{
		Role:      "agent",
		Text:      text,
		Timestamp: time.Now(),
	})

	// Send audio
	p.session.logger.Info("sending greeting audio to Telnyx", "bytes", len(audio))
	if err := p.sendAudioToTelnyx(audio); err != nil {
		p.session.logger.Error("failed to send greeting audio to Telnyx", "error", err)
		return fmt.Errorf("send greeting audio: %w", err)
	}

	p.session.logger.Info("greeting sent successfully")
	p.session.emitEvent(EventAgentSpeechEnd, nil)
	return nil
}

// Stop stops the pipeline.
func (p *Pipeline) Stop() {
	if p.cancel != nil {
		p.cancel()
	}
	close(p.done)
}

// Interrupt stops current speech generation.
func (p *Pipeline) Interrupt() {
	p.mu.Lock()
	p.interrupted = true
	p.mu.Unlock()

	// Clear the audio buffer on Telnyx side
	if p.conn != nil {
		_ = p.conn.clear()
	}
}

// ProcessText directly processes text input (bypasses STT).
func (p *Pipeline) ProcessText(ctx context.Context, text string) error {
	return p.processInput(ctx, text)
}

// processAudioLoop continuously processes incoming audio.
func (p *Pipeline) processAudioLoop() {
	// Signal that pipeline is ready to receive audio
	if p.conn.pipelineReady != nil {
		close(p.conn.pipelineReady)
	}

	for {
		select {
		case <-p.ctx.Done():
			return
		case audio, ok := <-p.conn.AudioIn():
			if !ok {
				return
			}
			p.handleAudio(audio)
		}
	}
}

// handleAudio processes incoming audio data.
func (p *Pipeline) handleAudio(audio []byte) {
	// Check for voice activity (simple energy-based detection)
	if hasVoiceActivity(audio) {
		p.bufferMu.Lock()
		if !p.isSpeaking {
			p.isSpeaking = true
			p.session.logger.Info("voice activity detected - user started speaking")
			p.session.emitEvent(EventUserSpeechStart, nil)
		}
		p.audioBuffer.Write(audio)
		p.lastSpeechTime = time.Now()
		bufferSize := p.audioBuffer.Len()
		p.bufferMu.Unlock()

		// Log buffer size periodically (every ~1 second of audio at 8kHz)
		if bufferSize%8000 < 160 {
			p.session.logger.Debug("audio buffer growing", "bytes", bufferSize)
		}

		// If agent is speaking and interruption is enabled, interrupt
		p.mu.RLock()
		mode := p.session.gateway.config.InterruptionMode
		isProcessing := p.isProcessing
		p.mu.RUnlock()

		if isProcessing && mode == "immediate" {
			p.Interrupt()
		}
	}
}

// silenceDetectionLoop detects end of speech based on silence.
func (p *Pipeline) silenceDetectionLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.bufferMu.Lock()
			if p.isSpeaking && time.Since(p.lastSpeechTime) > p.silenceTimeout {
				// End of speech detected
				p.isSpeaking = false
				audio := p.audioBuffer.Bytes()
				p.audioBuffer.Reset()
				p.bufferMu.Unlock()

				p.session.logger.Info("silence detected - user stopped speaking",
					"audio_bytes", len(audio),
					"silence_duration_ms", p.silenceTimeout.Milliseconds())
				p.session.emitEvent(EventUserSpeechEnd, nil)

				// Process the collected audio
				if len(audio) > 0 {
					p.session.logger.Info("sending audio to STT", "bytes", len(audio))
					go p.processAudio(audio)
				} else {
					p.session.logger.Warn("no audio collected, skipping STT")
				}
			} else {
				p.bufferMu.Unlock()
			}
		}
	}
}

// processAudio transcribes audio and processes the result.
func (p *Pipeline) processAudio(audio []byte) {
	p.mu.Lock()
	p.isProcessing = true
	p.interrupted = false
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		p.isProcessing = false
		p.mu.Unlock()
	}()

	startTime := time.Now()
	p.session.logger.Info("processing audio", "bytes", len(audio))

	// STT: Convert audio to text
	p.session.logger.Info("calling STT provider", "provider", p.session.gateway.config.STTProvider)
	sttStart := time.Now()
	text, err := p.stt.Transcribe(p.ctx, audio)
	sttLatency := time.Since(sttStart)

	if err != nil {
		p.session.logger.Error("STT error", "error", err, "latency_ms", sttLatency.Milliseconds())
		p.session.emitEvent(EventError, err)
		return
	}

	p.session.logger.Info("STT complete", "latency_ms", sttLatency.Milliseconds(), "text", text)

	if text == "" {
		p.session.logger.Warn("STT returned empty text, skipping LLM")
		return
	}

	p.session.logger.Info("user transcript", "text", text)
	p.session.emitEvent(EventUserTranscript, text)

	// Add user turn to transcript
	p.session.addTurn(Turn{
		Role:       "user",
		Text:       text,
		Timestamp:  time.Now(),
		DurationMs: int(time.Since(startTime).Milliseconds()),
	})

	// Check if interrupted
	p.mu.RLock()
	if p.interrupted {
		p.mu.RUnlock()
		return
	}
	p.mu.RUnlock()

	// LLM: Generate response
	p.session.logger.Info("calling LLM provider", "provider", p.session.gateway.config.LLMProvider)
	p.session.emitEvent(EventAgentThinking, nil)
	llmStart := time.Now()
	response, toolCalls, err := p.llm.Generate(p.ctx, text, p.session.Transcript())
	llmLatency := time.Since(llmStart)

	if err != nil {
		p.session.logger.Error("LLM error", "error", err, "latency_ms", llmLatency.Milliseconds())
		p.session.emitEvent(EventError, err)
		return
	}

	p.session.logger.Info("LLM complete", "latency_ms", llmLatency.Milliseconds(), "response_len", len(response))

	// Check if interrupted
	p.mu.RLock()
	if p.interrupted {
		p.mu.RUnlock()
		return
	}
	p.mu.RUnlock()

	p.session.logger.Info("agent response", "text", response)
	p.session.emitEvent(EventAgentTranscript, response)

	// Handle tool calls
	for _, tc := range toolCalls {
		p.session.emitEvent(EventToolCall, tc)
		p.session.mu.Lock()
		p.session.metrics.ToolCallCount++
		p.session.mu.Unlock()
	}

	// TTS: Synthesize speech
	p.session.logger.Info("calling TTS provider", "provider", p.session.gateway.config.TTSProvider)
	p.session.emitEvent(EventAgentSpeechStart, nil)
	ttsStart := time.Now()
	audio, err = p.tts.Synthesize(p.ctx, response)
	ttsLatency := time.Since(ttsStart)

	if err != nil {
		p.session.logger.Error("TTS error", "error", err, "latency_ms", ttsLatency.Milliseconds())
		p.session.emitEvent(EventError, err)
		return
	}

	p.session.logger.Info("TTS complete", "latency_ms", ttsLatency.Milliseconds(), "audio_bytes", len(audio))

	// Add agent turn to transcript
	p.session.addTurn(Turn{
		Role:       "agent",
		Text:       response,
		Timestamp:  time.Now(),
		DurationMs: int(time.Since(llmStart).Milliseconds()),
		ToolCalls:  toolCalls,
	})

	// Update metrics
	p.session.updateMetrics(sttLatency, llmLatency, ttsLatency)

	// Check if interrupted before sending audio
	p.mu.RLock()
	if p.interrupted {
		p.mu.RUnlock()
		p.session.emitEvent(EventAgentSpeechEnd, nil)
		return
	}
	p.mu.RUnlock()

	// Send audio to Telnyx
	p.session.logger.Info("sending audio to Telnyx", "bytes", len(audio))
	if err := p.sendAudioToTelnyx(audio); err != nil {
		p.session.logger.Error("failed to send audio", "error", err)
	}

	totalLatency := time.Since(startTime)
	p.session.logger.Info("turn complete",
		"stt_ms", sttLatency.Milliseconds(),
		"llm_ms", llmLatency.Milliseconds(),
		"tts_ms", ttsLatency.Milliseconds(),
		"total_ms", totalLatency.Milliseconds())

	p.session.emitEvent(EventAgentSpeechEnd, nil)
}

// processInput processes text input directly (bypasses STT).
func (p *Pipeline) processInput(ctx context.Context, text string) error {
	p.mu.Lock()
	p.isProcessing = true
	p.interrupted = false
	p.mu.Unlock()

	defer func() {
		p.mu.Lock()
		p.isProcessing = false
		p.mu.Unlock()
	}()

	// Add user turn
	p.session.addTurn(Turn{
		Role:      "user",
		Text:      text,
		Timestamp: time.Now(),
	})

	// LLM: Generate response
	p.session.emitEvent(EventAgentThinking, nil)
	response, toolCalls, err := p.llm.Generate(ctx, text, p.session.Transcript())
	if err != nil {
		return err
	}

	p.session.emitEvent(EventAgentTranscript, response)

	// TTS: Synthesize speech
	p.session.emitEvent(EventAgentSpeechStart, nil)
	audio, err := p.tts.Synthesize(ctx, response)
	if err != nil {
		return err
	}

	// Add agent turn
	p.session.addTurn(Turn{
		Role:      "agent",
		Text:      response,
		Timestamp: time.Now(),
		ToolCalls: toolCalls,
	})

	// Send audio
	if err := p.sendAudioToTelnyx(audio); err != nil {
		return err
	}

	p.session.emitEvent(EventAgentSpeechEnd, nil)
	return nil
}

// sendAudioToTelnyx sends audio data to Telnyx in chunks.
func (p *Pipeline) sendAudioToTelnyx(audio []byte) error {
	// Telnyx expects μ-law encoded audio at 8kHz
	// The audio from TTS providers may need to be converted
	// For now, we'll send it as-is and rely on the TTS provider
	// to output the correct format

	// Send in chunks (Telnyx recommends ~20ms chunks for 8kHz = 160 samples = 160 bytes for μ-law)
	// Use larger chunks (40ms = 320 bytes) to reduce overhead and improve smoothness
	const chunkSize = 320
	const chunkDuration = 40 * time.Millisecond

	startTime := time.Now()

	for i := 0; i < len(audio); i += chunkSize {
		// Check if interrupted
		p.mu.RLock()
		if p.interrupted {
			p.mu.RUnlock()
			return nil
		}
		p.mu.RUnlock()

		end := i + chunkSize
		if end > len(audio) {
			end = len(audio)
		}

		// Make a copy of the chunk to avoid race conditions
		chunk := make([]byte, end-i)
		copy(chunk, audio[i:end])

		// Block until we can send - don't drop audio
		select {
		case p.conn.audioOut <- chunk:
		case <-p.ctx.Done():
			return p.ctx.Err()
		}

		// Pace audio to match real-time playback
		// Calculate expected time for this chunk based on bytes sent
		chunkIndex := i / chunkSize
		expectedTime := time.Duration(chunkIndex+1) * chunkDuration
		elapsed := time.Since(startTime)
		if sleepTime := expectedTime - elapsed; sleepTime > 0 {
			time.Sleep(sleepTime)
		}
	}

	return nil
}

// hasVoiceActivity performs simple voice activity detection on μ-law audio.
func hasVoiceActivity(audio []byte) bool {
	if len(audio) == 0 {
		return false
	}

	// Decode μ-law to linear PCM for accurate energy calculation
	// μ-law is logarithmic, so we can't just subtract 128
	pcm := codec.MulawDecode(audio)

	// Calculate RMS energy on linear PCM
	var sum int64
	for _, sample := range pcm {
		sum += int64(sample) * int64(sample)
	}
	rms := sum / int64(len(pcm))

	// Threshold for voice activity
	// For 16-bit PCM, silence is ~0, speech is typically > 500000 (RMS ~700)
	// Adjust threshold based on testing - 100000 is conservative
	return rms > 100000
}
