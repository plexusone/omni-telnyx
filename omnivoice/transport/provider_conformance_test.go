//go:build integration

package transport

import (
	"testing"

	"github.com/plexusone/omnivoice-core/transport/providertest"
)

func TestConformance(t *testing.T) {
	p, err := New()
	if err != nil {
		t.Fatal(err)
	}

	// Transport tests are skipped for integration since they require
	// an active WebSocket connection from Telnyx Media Streaming.
	// Unit tests verify interface compliance.
	providertest.RunAll(t, providertest.Config{
		Provider:        p,
		SkipIntegration: true,
	})
}
