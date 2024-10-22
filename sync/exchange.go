package sync

import (
	"context"

	v1 "github.com/bredtape/gateway/sync/v1"
)

type Exchange interface {
	StartReceiving(context.Context) (<-chan *v1.MessageBatch, error)
	Write(context.Context, *v1.MessageBatch) error
}
