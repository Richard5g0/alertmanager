package webhook

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestWebhookGoroutineLeak(t *testing.T) {
	// Verify that no goroutines are leaked.
	defer goleak.VerifyNone(t)

	// Create a mock server that hangs indefinitely.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			// Request cancelled/timed out.
		case <-time.After(2 * time.Second):
			// Simulate slow response.
		}
	}))
	defer server.Close()

	conf := &Config{
		URL:     server.URL,
		Timeout: 100 * time.Millisecond,
	}
	notifier := New(conf, nil)

	ctx := context.Background()
	alert := &Alert{
		Status: "firing",
		Labels: map[string]string{"alertname": "TestAlert"},
	}

	// Trigger the notification. It should time out after 100ms.
	_, err := notifier.Notify(ctx, alert)
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}
