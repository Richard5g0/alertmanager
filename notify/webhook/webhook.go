package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Config holds the configuration for the Webhook notifier.
type Config struct {
	URL     string        `yaml:"url" json:"url"`
	Timeout time.Duration `yaml:"timeout" json:"timeout"`
}

// Notifier implements the notifier interface for Webhook.
type Notifier struct {
	conf   *Config
	client *http.Client
}

// New returns a new Webhook notifier.
func New(conf *Config, client *http.Client) *Notifier {
	if client == nil {
		client = &http.Client{}
	}
	return &Notifier{
		conf:   conf,
		client: client,
	}
}

// Alert represents a simplified alert structure.
type Alert struct {
	Status       string            `json:"status"`
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	StartsAt     time.Time         `json:"startsAt"`
	EndsAt       time.Time         `json:"endsAt"`
	GeneratorURL string            `json:"generatorURL"`
}

// Message represents the payload sent to the webhook.
type Message struct {
	Version           string            `json:"version"`
	GroupKey          string            `json:"groupKey"`
	Status            string            `json:"status"`
	Receiver          string            `json:"receiver"`
	GroupLabels       map[string]string `json:"groupLabels"`
	CommonLabels      map[string]string `json:"commonLabels"`
	CommonAnnotations map[string]string `json:"commonAnnotations"`
	ExternalURL       string            `json:"externalURL"`
	Alerts            []Alert           `json:"alerts"`
}

// Notify sends the alerts to the configured webhook URL.
func (n *Notifier) Notify(ctx context.Context, alerts ...*Alert) (bool, error) {
	var cancel context.CancelFunc
	if n.conf.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, n.conf.Timeout)
		defer cancel()
	}

	msg := Message{
		Version: "4",
		Status:  "firing",
		Alerts:  make([]Alert, len(alerts)),
	}
	for i, a := range alerts {
		msg.Alerts[i] = *a
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return false, fmt.Errorf("marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.conf.URL, bytes.NewReader(payload))
	if err != nil {
		return false, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	type result struct {
		resp *http.Response
		err  error
	}

	// Use a buffered channel to prevent goroutine leak if the context is cancelled/times out
	// before the HTTP request completes.
	ch := make(chan result, 1)

	go func() {
		resp, err := n.client.Do(req)
		ch <- result{resp: resp, err: err}
	}()

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case res := <-ch:
		if res.err != nil {
			return false, res.err
		}
		defer res.resp.Body.Close()
		if res.resp.StatusCode < 200 || res.resp.StatusCode >= 300 {
			return false, fmt.Errorf("unexpected status code %d", res.resp.StatusCode)
		}
		return true, nil
	}
}
