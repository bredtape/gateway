package nats_sync

import (
	"context"
	"time"

	v1 "github.com/bredtape/gateway/nats_sync/v1"
	"github.com/pkg/errors"
)

type HTTPExchangeConfig struct {
	ServeAddress   string
	ServePath      string
	ReceiveTimeout time.Duration

	ClientAddress string
	ClientPath    string
}

func (c HTTPExchangeConfig) Validate() error {
	if c.ServeAddress == "" {
		return errors.New("serveAddress empty")
	}
	if c.ServePath == "" {
		return errors.New("servePath empty")
	}
	if c.ReceiveTimeout < time.Second {
		return errors.New("receiveTimeout must be at least 1 sec")
	}
	if c.ClientAddress == "" {
		return errors.New("clientAddress empty")
	}
	if c.ClientPath == "" {
		return errors.New("clientPath empty")
	}
	return nil
}

// HTTPExchange expects incoming v1.MessageExhange to be posted to the hosted endpoint
// and acts as a client to the specified HTTP endpoint
// HTTP headers must have:
// * content-type: application/protobuf
// * grpc-message-type: com.github.bredtape.gateway.nats_sync.v1.MessageExchange
type HTTPExchange struct {
}

func (ex *HTTPExchange) StartReceiving(ctx context.Context) (<-chan *v1.MessageBatch, error) {
	return nil, errors.New("not implemented")
}

func (ex *HTTPExchange) Write(ctx context.Context, msg *v1.MessageBatch) error {
	return errors.New("not implemented")
}
