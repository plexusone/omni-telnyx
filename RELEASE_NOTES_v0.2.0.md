# Release Notes: v0.2.0

**Release Date:** 2026-05-02

## Summary

Module rename from `omnivoice-telnyx` to `omni-telnyx` with package restructure to align with the omni-* ecosystem naming convention.

## Breaking Changes

| Component | Before | After |
|-----------|--------|-------|
| Go module | `github.com/plexusone/omnivoice-telnyx` | `github.com/plexusone/omni-telnyx` |
| callsystem | `omnivoice-telnyx/callsystem` | `omni-telnyx/omnivoice/callsystem` |
| transport | `omnivoice-telnyx/transport` | `omni-telnyx/omnivoice/transport` |

## Migration Guide

Update your import paths:

```go
// Before
import "github.com/plexusone/omnivoice-telnyx/callsystem"
import "github.com/plexusone/omnivoice-telnyx/transport"

// After
import "github.com/plexusone/omni-telnyx/omnivoice/callsystem"
import "github.com/plexusone/omni-telnyx/omnivoice/transport"
```

Update your `go.mod`:

```bash
go mod edit -droprequire github.com/plexusone/omnivoice-telnyx
go get github.com/plexusone/omni-telnyx@v0.2.0
go mod tidy
```

## Package Structure

The new structure follows the omni-* ecosystem pattern:

```
omni-telnyx/
├── omnivoice/
│   ├── callsystem/    # CallSystem and SMSProvider
│   │   ├── provider.go
│   │   ├── call.go
│   │   └── provider_conformance_test.go
│   └── transport/     # Media Streaming transport
│       ├── provider.go
│       └── provider_conformance_test.go
├── go.mod
└── README.md
```

## Tests

Conformance tests using `omnivoice-core/providertest`:

```bash
# Run with integration tag (skips if no API key)
go test -tags integration ./...

# With environment variables for full integration
export TELNYX_API_KEY="your-key"
export TELNYX_PHONE_NUMBER="+15551234567"
export TELNYX_TO_NUMBER="+15559876543"
export TELNYX_CONNECTION_ID="your-connection-id"
go test -tags integration ./...
```

## Related

This rename aligns with the omni-* ecosystem:

- [omni-openai](https://github.com/plexusone/omni-openai) - OpenAI provider
- [omni-deepgram](https://github.com/plexusone/omni-deepgram) - Deepgram provider
- [omnivoice-core](https://github.com/plexusone/omnivoice-core) - Core interfaces

## Full Changelog

See [CHANGELOG.md](CHANGELOG.md) for the complete list of changes.
