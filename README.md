# Omni-Telnyx

[![Go CI][go-ci-svg]][go-ci-url]
[![Go Lint][go-lint-svg]][go-lint-url]
[![Go SAST][go-sast-svg]][go-sast-url]
[![Go Report Card][goreport-svg]][goreport-url]
[![Docs][docs-godoc-svg]][docs-godoc-url]
[![Visualization][viz-svg]][viz-url]
[![License][license-svg]][license-url]

 [go-ci-svg]: https://github.com/plexusone/omni-telnyx/actions/workflows/go-ci.yaml/badge.svg?branch=main
 [go-ci-url]: https://github.com/plexusone/omni-telnyx/actions/workflows/go-ci.yaml
 [go-lint-svg]: https://github.com/plexusone/omni-telnyx/actions/workflows/go-lint.yaml/badge.svg?branch=main
 [go-lint-url]: https://github.com/plexusone/omni-telnyx/actions/workflows/go-lint.yaml
 [go-sast-svg]: https://github.com/plexusone/omni-telnyx/actions/workflows/go-sast-codeql.yaml/badge.svg?branch=main
 [go-sast-url]: https://github.com/plexusone/omni-telnyx/actions/workflows/go-sast-codeql.yaml
 [goreport-svg]: https://goreportcard.com/badge/github.com/plexusone/omni-telnyx
 [goreport-url]: https://goreportcard.com/report/github.com/plexusone/omni-telnyx
 [docs-godoc-svg]: https://pkg.go.dev/badge/github.com/plexusone/omni-telnyx
 [docs-godoc-url]: https://pkg.go.dev/github.com/plexusone/omni-telnyx
 [viz-svg]: https://img.shields.io/badge/visualizaton-Go-blue.svg
 [viz-url]: https://mango-dune-07a8b7110.1.azurestaticapps.net/?repo=plexusone%2Fomni-telnyx
 [loc-svg]: https://tokei.rs/b1/github/plexusone/omni-telnyx
 [repo-url]: https://github.com/plexusone/omni-telnyx
 [license-svg]: https://img.shields.io/badge/license-MIT-blue.svg
 [license-url]: https://github.com/plexusone/omni-telnyx/blob/main/LICENSE

Telnyx provider implementation for [OmniVoice](https://github.com/plexusone/omnivoice-core) - the voice abstraction layer for PlexusOne.

## Features

- 📞 **CallSystem**: PSTN call handling via Telnyx Call Control API
- 📡 **Transport**: Telnyx Media Streaming for real-time audio
- 🎙️ **Voice Gateway**: Full STT→LLM→TTS pipeline with registry integration
- 🔌 **Provider Injection**: Inject your own LLM providers for full SDK access
- 💬 **SMS/MMS/RCS**: Messaging via SMSProvider and OmniChat interfaces

## Installation

```bash
go get github.com/plexusone/omni-telnyx
```

## Quick Start

### Making Outbound Calls

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/plexusone/omni-telnyx/omnivoice/callsystem"
)

func main() {
    // Create Telnyx provider
    provider, err := callsystem.New(
        callsystem.WithAPIKey("YOUR_TELNYX_API_KEY"),
        callsystem.WithPhoneNumber("+15551234567"),
        callsystem.WithConnectionID("your-connection-id"),
        callsystem.WithWebhookURL("https://your-server.com/webhooks"),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer provider.Close()

    // Make outbound call
    call, err := provider.MakeCall(context.Background(), "+15559876543")
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Call initiated: %s\n", call.ID())
}
```

### Handling Incoming Calls

```go
import (
    "net/http"

    "github.com/plexusone/omnivoice-core/callsystem"
    telnyxcs "github.com/plexusone/omni-telnyx/omnivoice/callsystem"
)

func main() {
    provider, _ := telnyxcs.New(
        telnyxcs.WithAPIKey("YOUR_TELNYX_API_KEY"),
        telnyxcs.WithPhoneNumber("+15551234567"),
    )

    // Set incoming call handler
    provider.OnIncomingCall(func(call callsystem.Call) error {
        fmt.Printf("Incoming call from %s\n", call.From())
        return call.Answer(context.Background())
    })

    // Handle Telnyx webhooks
    http.HandleFunc("/webhooks", func(w http.ResponseWriter, r *http.Request) {
        // Parse Telnyx webhook payload
        // callControlID, from, to := parseWebhook(r)
        // provider.HandleIncomingWebhook(callControlID, from, to)
    })

    http.ListenAndServe(":8080", nil)
}
```

### Media Streaming

```go
import "github.com/plexusone/omni-telnyx/omnivoice/transport"

// Create transport for Media Streaming
tr, _ := transport.New()

// Listen for Media Streaming connections
connCh, _ := tr.Listen(ctx, "/media-stream")

// Handle WebSocket upgrades in your HTTP handler
http.HandleFunc("/media-stream", func(w http.ResponseWriter, r *http.Request) {
    tr.HandleWebSocket(w, r, "/media-stream")
})

// Process connections
for conn := range connCh {
    go func(c transport.Connection) {
        // Read audio from caller
        audio := make([]byte, 1024)
        for {
            n, err := c.AudioOut().Read(audio)
            if err != nil {
                break
            }
            // Process audio...

            // Send audio back
            c.AudioIn().Write(responseAudio)
        }
    }(conn)
}
```

### Call Control Features

```go
// After call is answered...
call, _ := provider.MakeCall(ctx, "+15559876543")

// Start media streaming
telnyxCall := call.(*telnyxcs.Call)
telnyxCall.StartMediaStreaming(ctx, "wss://your-server.com/media-stream")

// Use built-in TTS
telnyxCall.Speak(ctx, "Hello, how can I help you?", "", "")

// Play audio file
telnyxCall.PlayAudio(ctx, "https://example.com/audio.mp3")

// Start real-time transcription
telnyxCall.StartTranscription(ctx, "en")

// Stop transcription
telnyxCall.StopTranscription(ctx)

// Hang up
call.Hangup(ctx)
```

### SMS Messaging

```go
// Send SMS using default number
msg, err := provider.SendSMS(ctx, "+15559876543", "Hello from OmniVoice!")

// Send SMS from specific number
msg, err = provider.SendSMSFrom(ctx, "+15559876543", "+15551234567", "Hello!")

fmt.Printf("Message sent: %s\n", msg.ID)
```

### Registry Integration

Use omnivoice-core's provider registry for automatic discovery:

```go
import (
    omnivoice "github.com/plexusone/omnivoice-core"
    "github.com/plexusone/omnivoice-core/registry"
    _ "github.com/plexusone/omni-telnyx/omnivoice/gateway" // Auto-register
)

// Get gateway via registry
gw, err := omnivoice.GetGatewayProvider("telnyx",
    registry.WithAPIKey("TELNYX_API_KEY"),
    registry.WithExtension("phoneNumber", "+15551234567"),
)
```

### LLM Client Injection

Inject your own thick LLM provider instead of using the default:

```go
import (
    "github.com/plexusone/omni-telnyx/omnivoice/gateway"
    "github.com/plexusone/omnillm" // Your thick provider
)

// Create LLM client with full SDK access
llmClient, _ := omnillm.NewClient(omnillm.ClientConfig{
    Providers: []omnillm.ProviderConfig{{
        Provider: "anthropic",
        APIKey:   os.Getenv("ANTHROPIC_API_KEY"),
    }},
})

// Inject into gateway config
gw, err := gateway.New(gateway.Config{
    APIKey:    "TELNYX_API_KEY",
    LLMClient: llmClient,
})
```

## Configuration

### Environment Variables

```bash
export TELNYX_API_KEY="your-api-key"
```

### Options

```go
provider, _ := callsystem.New(
    callsystem.WithAPIKey("your-api-key"),
    callsystem.WithPhoneNumber("+15551234567"),      // Default outbound number
    callsystem.WithConnectionID("connection-id"),    // Telnyx Connection ID
    callsystem.WithWebhookURL("https://..."),        // Webhook URL for events
)
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Phone Call Flow                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Caller ←→ Telnyx PSTN ←→ Media Streaming ←→ Your Server    │
│                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────────┐  │
│  │ CallSystem  │    │  Transport  │    │     Agent       │  │
│  │  (calls)    │←──→│  (audio)    │←──→│   (TTS/STT)     │  │
│  └─────────────┘    └─────────────┘    └─────────────────┘  │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Telnyx Concepts

### Call Control ID

Every call has a unique `call_control_id` used to send commands via the Call Control API.

### Connection ID

A Telnyx Connection represents a configuration for handling calls (SIP, PSTN, etc.). Required for outbound calls.

### Media Streaming

Real-time bidirectional audio via WebSocket. Audio is base64-encoded mu-law or a-law at 8kHz.

## Requirements

- Go 1.21+
- Telnyx Account with API Key
- Telnyx Connection ID (for outbound calls)
- Public webhook URL for call events
- WebSocket endpoint for Media Streaming

## Testing

Run conformance tests:

```bash
# Unit tests (no API key required)
go test ./...

# Integration tests (requires API key)
export TELNYX_API_KEY="your-api-key"
export TELNYX_PHONE_NUMBER="+15551234567"
export TELNYX_TO_NUMBER="+15559876543"
export TELNYX_CONNECTION_ID="your-connection-id"
go test -tags integration ./...
```

## Related Packages

- [omnivoice-core](https://github.com/plexusone/omnivoice-core) - Core interfaces
- [twilio-go](https://github.com/plexusone/twilio-go) - Twilio provider
- [elevenlabs-go](https://github.com/plexusone/elevenlabs-go) - ElevenLabs SDK with OmniVoice provider

## License

MIT
