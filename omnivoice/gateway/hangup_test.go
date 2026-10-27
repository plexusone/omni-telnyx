package gateway

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/team-telnyx/telnyx-go/v4"
	"github.com/team-telnyx/telnyx-go/v4/option"

	coregateway "github.com/plexusone/omnivoice-core/gateway"
)

func TestHangupEndsCallAndClosesSession(t *testing.T) {
	fake := &fakeCallControl{bodies: map[string]map[string]any{}}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)

	g := newTestGateway(t, coregateway.PipelineModeAudio)
	*g.callSystem.Client() = telnyx.NewClient(
		option.WithAPIKey("test-key"),
		option.WithBaseURL(srv.URL+"/"),
		option.WithMaxRetries(0),
	)
	g.createSession(testCallID, "+15550000000", "+16695550100", "outbound")

	sess, ok := g.GetSession(testCallID)
	if !ok {
		t.Fatal("session not found")
	}
	hanger, ok := sess.(interface{ Hangup(context.Context) error })
	if !ok {
		t.Fatal("audio mode session does not implement Hangup")
	}
	if err := hanger.Hangup(context.Background()); err != nil {
		t.Fatalf("Hangup() error: %v", err)
	}

	if _, ok := fake.body("/calls/" + testCallID + "/actions/hangup"); !ok {
		t.Fatal("Hangup did not send the hangup command")
	}
	if _, ok := g.GetSession(testCallID); ok {
		t.Fatal("session still registered after Hangup")
	}
}
