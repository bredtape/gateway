package sync

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	v1 "github.com/bredtape/gateway/sync/v1"
	"google.golang.org/protobuf/proto"
)

func TestHTTPExchange_ServeHTTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	cfg := HTTPExchangeConfig{
		From:      "a",
		To:        "b",
		ClientURL: "http://example.com",
	}
	ex, err := NewHTTPExchange(cfg)
	if err != nil {
		t.Fatalf("failed to create HTTPExchange: %v", err)
	}

	msg := &v1.MessageBatch{
		FromDeployment: "b",
		ToDeployment:   "a"}
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("failed to marshal message: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(data))
	req.Header.Set("content-type", "application/protobuf")
	req.Header.Set("protobuf-message-type", "com.github.bredtape.gateway.nats_sync.v1.MessageBatch")
	req.Header.Set("digest", hashSHA256AndBase64(data))

	w := httptest.NewRecorder()

	ex.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Logf("response body: %s", body)
		t.Errorf("expected status OK; got %v", resp.Status)
	}

	ch, _ := ex.StartReceiving(ctx)
	select {
	case msg, ok := <-ch:
		if !ok {
			t.Error("channel closed")
		}

		if msg == nil {
			t.Fatal("nil message")
		}

		var restored v1.MessageBatch
		err := proto.Unmarshal(data, &restored)
		if err != nil {
			t.Fatalf("failed to unmarshal message: %v", err)
		}

		if restored.FromDeployment != msg.FromDeployment {
			t.Errorf("expected from %q; got %q", msg.FromDeployment, restored.FromDeployment)
		}
		if restored.ToDeployment != msg.ToDeployment {
			t.Errorf("expected to %q; got %q", msg.ToDeployment, restored.ToDeployment)
		}

	default:
		t.Error("expected message")
	}
}

func TestHTTPExchangeWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	exReceiver, err := NewHTTPExchange(HTTPExchangeConfig{From: "a", To: "b", ClientURL: "dont care"})
	if err != nil {
		t.Fatalf("failed to create HTTPExchange: %v", err)
	}

	server := httptest.NewServer(exReceiver)
	defer server.Close()

	cfg := HTTPExchangeConfig{
		From:      "a",
		To:        "b",
		ClientURL: server.URL}

	exSender, err := newHTTPExchange(cfg, server.Client())
	if err != nil {
		t.Fatalf("failed to create HTTPExchange: %v", err)
	}

	msg := &v1.MessageBatch{
		FromDeployment: "b",
		ToDeployment:   "a"}

	err = exSender.Write(ctx, msg)
	if err != nil {
		t.Fatalf("failed to write message: %v", err)
	}

	ch, _ := exReceiver.StartReceiving(ctx)
	select {
	case msg, ok := <-ch:
		if !ok {
			t.Error("channel closed")
		}

		if msg == nil {
			t.Fatal("nil message")
		}

		if msg.FromDeployment != "b" {
			t.Errorf("expected from b; got %q", msg.FromDeployment)
		}
		if msg.ToDeployment != "a" {
			t.Errorf("expected to a; got %q", msg.ToDeployment)
		}

	default:
		t.Error("expected message")
	}
}
