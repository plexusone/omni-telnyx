//go:build integration

package callsystem

import (
	"os"
	"testing"

	"github.com/plexusone/omnivoice-core/callsystem/providertest"
)

func TestConformance(t *testing.T) {
	apiKey := os.Getenv("TELNYX_API_KEY")
	from := os.Getenv("TELNYX_PHONE_NUMBER")
	to := os.Getenv("TELNYX_TO_NUMBER")
	connID := os.Getenv("TELNYX_CONNECTION_ID")

	if apiKey == "" {
		t.Skip("TELNYX_API_KEY not set")
	}

	p, err := New(
		WithAPIKey(apiKey),
		WithPhoneNumber(from),
		WithConnectionID(connID),
	)
	if err != nil {
		t.Fatal(err)
	}

	providertest.RunAll(t, providertest.Config{
		Provider:        p,
		SkipIntegration: from == "" || to == "" || connID == "",
		TestPhoneNumber: to,
		TestFromNumber:  from,
	})
}
