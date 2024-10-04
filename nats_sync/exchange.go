package nats_sync

import (
	"context"

	v1 "github.com/bredtape/gateway/nats_sync/v1"
)

type Exchange interface {
	StartReceiving(context.Context) (<-chan *v1.MessageExchange, error)
	Write(context.Context, *v1.MessageExchange) error
}
