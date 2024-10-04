package nats_sync

import (
	"context"
	"crypto/tls"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
)

type JSConfig struct {
	// nats urls, separated by ,
	NatsURLS string

	NatsOptions []nats.Option

	// optional prefix for jetstream api
	JetstreamAPIPrefix string
}

func (c *JSConfig) WithSeedFile(seedFile string) error {
	opt, err := nats.NkeyOptionFromSeed(seedFile)
	if err != nil {
		return err
	}
	c.NatsOptions = append(c.NatsOptions, opt)
	return nil
}

func (c *JSConfig) WithSecure(secure *tls.Config) error {
	c.NatsOptions = append(c.NatsOptions, nats.Secure(secure))
	return nil
}

// nats jetstream connection
type JSConn struct {
	config JSConfig

	// chan with cap 1 to lock js
	jsLock chan struct{}
	js     *jetstream.JetStream
}

func NewJSConn(config JSConfig) *JSConn {
	return &JSConn{
		config: config,
		jsLock: make(chan struct{}, 1),
	}
}

// acquire connection to jetstream. Do not modify returned jetstream reference
func (c *JSConn) Connect(ctx context.Context) (jetstream.JetStream, error) {
	// try to acquire lock
	select {
	case c.jsLock <- struct{}{}:
		// acquired lock

		// defer unlock
		defer func() {
			<-c.jsLock
		}()

		// already connected
		if c.js != nil {
			return *c.js, nil
		}

	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// no existing connection, connect
	nc, err := nats.Connect(c.config.NatsURLS, c.config.NatsOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect or bad options")
	}

	if len(c.config.JetstreamAPIPrefix) > 0 {
		js, err := jetstream.NewWithAPIPrefix(nc, c.config.JetstreamAPIPrefix)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create jetstream with api prefix")
		}
		c.js = &js
	} else {
		js, err := jetstream.New(nc)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create jetstream")
		}

		c.js = &js
	}
	return *c.js, nil
}
