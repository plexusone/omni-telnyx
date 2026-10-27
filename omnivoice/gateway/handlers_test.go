package gateway

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHandleInboundCallIgnoresOutgoingInitiated(t *testing.T) {
	g, err := New(Config{APIKey: "test-key", PhoneNumber: "+15550000000"})
	if err != nil {
		t.Fatalf("New() error: %v", err)
	}

	accepted := false
	g.OnCall(func(*CallInfo) error {
		accepted = true
		return nil
	})

	body := `{"data":{"event_type":"call.initiated","payload":{` +
		`"call_control_id":"v3:outbound","from":"+15550000000","to":"+15551234567","direction":"outgoing"}}}`
	req := httptest.NewRequest(http.MethodPost, "/voice/inbound", strings.NewReader(body))
	rec := httptest.NewRecorder()

	g.handleInboundCall(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if accepted {
		t.Fatal("outbound call.initiated was passed to the inbound call handler")
	}
	if _, ok := g.GetSession("v3:outbound"); ok {
		t.Fatal("outbound call.initiated created an inbound session")
	}
}
