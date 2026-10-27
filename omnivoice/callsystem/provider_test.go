package callsystem

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/team-telnyx/telnyx-go/v4"
	"github.com/team-telnyx/telnyx-go/v4/option"

	"github.com/plexusone/omnivoice-core/callsystem"
)

// fakeTelnyx records Call Control API requests.
type fakeTelnyx struct {
	mu       sync.Mutex
	requests []fakeRequest
}

type fakeRequest struct {
	Path string
	Body map[string]any
}

func (f *fakeTelnyx) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		body := map[string]any{}
		if len(raw) > 0 {
			if err := json.Unmarshal(raw, &body); err != nil {
				t.Errorf("decode body: %v", err)
			}
		}
		f.mu.Lock()
		f.requests = append(f.requests, fakeRequest{Path: r.URL.Path, Body: body})
		f.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/calls" {
			_, err = io.WriteString(w, `{"data":{"call_control_id":"v3:test-call","call_leg_id":"leg","call_session_id":"session"}}`)
		} else {
			_, err = io.WriteString(w, `{"data":{"result":"ok"}}`)
		}
		if err != nil {
			t.Errorf("write response: %v", err)
		}
	}
}

func (f *fakeTelnyx) paths() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	paths := make([]string, len(f.requests))
	for i, r := range f.requests {
		paths[i] = r.Path
	}
	return paths
}

func newTestProvider(t *testing.T) (*Provider, *fakeTelnyx) {
	t.Helper()
	fake := &fakeTelnyx{}
	srv := httptest.NewServer(fake.handler(t))
	t.Cleanup(srv.Close)

	p, err := New(WithAPIKey("test-key"), WithPhoneNumber("+15550000000"))
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}
	client := telnyx.NewClient(
		option.WithAPIKey("test-key"),
		option.WithBaseURL(srv.URL+"/"),
		option.WithMaxRetries(0),
	)
	p.client = &client
	return p, fake
}

func TestMakeCallSendsDigitsOnceWhenAnswered(t *testing.T) {
	p, fake := newTestProvider(t)
	ctx := context.Background()

	c, err := p.MakeCall(ctx, "+15551234567", callsystem.WithSendDigits("WW85012345678#"))
	if err != nil {
		t.Fatalf("MakeCall() error: %v", err)
	}
	if got := fake.paths(); len(got) != 1 || got[0] != "/calls" {
		t.Fatalf("requests after MakeCall = %v, want only /calls (digits wait for answer)", got)
	}

	call, ok := c.(*Call)
	if !ok {
		t.Fatalf("MakeCall returned %T, want *Call", c)
	}
	if err := call.SendPendingDigits(ctx); err != nil {
		t.Fatalf("SendPendingDigits() error: %v", err)
	}
	if err := call.SendPendingDigits(ctx); err != nil {
		t.Fatalf("second SendPendingDigits() error: %v", err)
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.requests) != 2 {
		t.Fatalf("requests = %d, want 2 (dial and one send_dtmf)", len(fake.requests))
	}
	dtmf := fake.requests[1]
	if dtmf.Path != "/calls/v3:test-call/actions/send_dtmf" {
		t.Fatalf("send_dtmf path = %q", dtmf.Path)
	}
	if dtmf.Body["digits"] != "WW85012345678#" {
		t.Fatalf("digits = %v, want %q", dtmf.Body["digits"], "WW85012345678#")
	}
	if dtmf.Body["command_id"] != "send-digits-v3:test-call" {
		t.Fatalf("command_id = %v, want %q", dtmf.Body["command_id"], "send-digits-v3:test-call")
	}
}

func TestSendPendingDigitsWithoutDigitsIsNoop(t *testing.T) {
	p, fake := newTestProvider(t)
	ctx := context.Background()

	c, err := p.MakeCall(ctx, "+15551234567")
	if err != nil {
		t.Fatalf("MakeCall() error: %v", err)
	}
	call, ok := c.(*Call)
	if !ok {
		t.Fatalf("MakeCall returned %T, want *Call", c)
	}
	if err := call.SendPendingDigits(ctx); err != nil {
		t.Fatalf("SendPendingDigits() error: %v", err)
	}
	if got := fake.paths(); len(got) != 1 {
		t.Fatalf("requests = %v, want only the dial request", got)
	}
}

func TestMakeCallRejectsInvalidDigits(t *testing.T) {
	p, fake := newTestProvider(t)

	if _, err := p.MakeCall(context.Background(), "+15551234567", callsystem.WithSendDigits("123,456#")); err == nil {
		t.Fatal("MakeCall() with invalid digits returned nil error")
	}
	if got := fake.paths(); len(got) != 0 {
		t.Fatalf("requests = %v, want none for invalid digits", got)
	}
}
