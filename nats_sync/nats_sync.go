package nats_sync

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/bredtape/gateway"
	v1 "github.com/bredtape/gateway/nats_sync/v1"
	"github.com/bredtape/retry"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

const (
	// subscription stream name placeholder used in published messages.
	// The actual stream may have a different name and is configured in the NatsSyncConfig for
	// each deployment
	subscriptionStream = "sync_subscriptions"
)

var (
	startSyncRequestName = string((&v1.StartSyncRequest{}).ProtoReflect().Descriptor().FullName())
	stopSyncRequestName  = string((&v1.StopSyncRequest{}).ProtoReflect().Descriptor().FullName())

	retryOp = retry.Must(retry.NewExp(0.2, time.Second, 10*time.Second))
)

type NatsSyncConfig struct {
	// 'this' deployment
	Deployment gateway.Deployment

	// Stream to persist start/stop sync requests.
	// Should exist, and must be replicated (by other means) to all deployments participating in the sync.
	// This implies that requests only can be accepted at the deployment that is the
	// source of the stream.
	// Assuming subjects: <sink deployment>.<source_deployment>.<source stream name>
	// Retention with MaxMsgsPerSubject can be used to limit the number of messages
	SyncStream string

	// communication settings pr sink deployment
	CommunicationSettings map[gateway.Deployment]CommunicationSettings

	// exchanges for each sink deployment. Must match the deployments in CommunicationSettings
	Exchanges map[gateway.Deployment]Exchange
}

func (c NatsSyncConfig) Validate() error {
	if c.Deployment == "" {
		return errors.New("deployment empty")
	}
	if c.SyncStream == "" {
		return errors.New("subscriptionStream empty")
	}

	if len(c.CommunicationSettings) == 0 {
		return errors.New("communicationSettings empty")
	}

	for d, s := range c.CommunicationSettings {
		if d == "" {
			return errors.New("empty deployment in communicationSettings")
		}
		if d == c.Deployment {
			return errors.New("deployment in communicationSettings is the same as the main deployment")
		}

		if ve := s.Validate(); ve != nil {
			return errors.Wrapf(ve, "invalid communicationSettings for deployment %s", d)
		}
	}

	if len(c.Exchanges) == 0 {
		return errors.New("exchanges empty")
	}

	if len(c.Exchanges) != len(c.CommunicationSettings) {
		return errors.New("exchanges and communicationSettings must have the same length")
	}

	for d, e := range c.Exchanges {
		if _, exists := c.CommunicationSettings[d]; !exists {
			return errors.Errorf("exchange for deployment %s does not have communicationSettings", d)
		}

		if e == nil {
			return errors.Errorf("exchange for deployment %s is nil", d)
		}
	}
	return nil
}

func StartNatsSync(ctx context.Context, js *JSConn, config NatsSyncConfig) error {
	if err := config.Validate(); err != nil {
		return errors.Wrap(err, "invalid config")
	}

	for d, cs := range config.CommunicationSettings {
		ns := &natsSync{
			from:       config.Deployment,
			to:         d,
			cs:         cs,
			syncStream: config.SyncStream,
			exchange:   config.Exchanges[d],
			js:         js}
		go ns.outerLoop(ctx)
	}

	return nil
}

// nats sync for from and to deployment (bidirectional)
type natsSync struct {
	from, to   gateway.Deployment
	cs         CommunicationSettings
	syncStream string
	exchange   Exchange

	js *JSConn
}

func (ns *natsSync) outerLoop(ctx context.Context) {
	r := retry.Must(retry.NewExp(0.5, time.Second, 10*time.Second))

	r.Try(ctx, func() error {
		log := slog.With("from", ns.from, "to", ns.to)

		ctxInner, cancel := context.WithCancel(ctx)
		err := ns.loop(ctxInner, log)
		cancel()
		if err != nil {
			log.Error("loop failed, will restart", "err", err)
		}
		return err
	})
}

func (ns *natsSync) loop(ctx context.Context, log *slog.Logger) error {
	state, err := newState(ns.from, ns.to, ns.cs, nil)
	if err != nil {
		return errors.Wrap(err, "failed to create state")
	}

	syncRequestsCh, err := ns.SubscribeToSync(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to subscribe to sync stream")
	}

	realIncomingCh, err := ns.exchange.StartReceiving(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to start receiving for deployment")
	}

	// incomingCh messages are initially ignored until existing sync requests have been processed
	incomingCh := make(<-chan *v1.MessageBatch)
	reachedSyncHead := false

	for {
		if reachedSyncHead {
			if state.HasSubscriptions() {
				incomingCh = realIncomingCh
			} else {
				log.Info("no subscriptions, waiting for sync requests")
			}
		}

		select {
		/*
			   also handle:
			   * changes to sync:
				   Apply to state, cleanup subscriptions and timers. Or maybe, restart everything else?

			   * flush timeout per sink deployment:
				  Could have a select-loop first which reads available messages (breaks when no messages available), then a select-loop which waits for the flush timeout. If no messages are available, but some buffered, send a flush message. Need to know when the first message per sink deployment was received. Also, if SourceDeliverFromLocal produces a ErrBackoff, when should we retry? Probably need another retry setting for this.

			   * heartbeat per sink deployment:
				  Could start a timer per sink deployment with the configured interval, then when it expires, check whether there has been any activity since the last heartbeat. If so, readjust the timer to last activity + interval. If not, send a heartbeat message.

				 * incoming backpressure:
				   If a state reports ErrBackoff for a sink deployment, the message received must be queued, but
					 future messages should not be received until the backoff is lifted. Would it be simpler to have a state instance per sink deployment?

		*/

		case <-ctx.Done():
		case msg, ok := <-incomingCh:
			if !ok {
				return errors.New("incoming channel closed")
			}

			if msg.GetToDeployment() != ns.from.String() {
				log.Warn("skipping message, not for this deployment", "msg", msg)
				continue
			}

			err := ns.handleIncomingMessage(ctx, msg)
			log.Error("failed to handle incoming message, ignoring", "err", err)

		case msg, ok := <-syncRequestsCh:
			if !ok {
				return errors.New("sync channel closed")
			}

			if msg.Error != nil {
				log.Warn("failed to receive sync request (ignoring)", "err", msg.Error)
				continue
			}

			// enable incoming messages when all existing sync requests have been processed
			if msg.Item.ReachedHead {
				reachedSyncHead = true
			}

			if msg.Item.StartSyncRequest != nil {
				err := state.RegisterStartSync(msg.Item.StartSyncRequest)
				if err != nil {
					log.Warn("failed to register start sync", "err", err,
						"sourceStreamName", msg.Item.StartSyncRequest.SourceStreamName,
						"sourceDeployment", msg.Item.StartSyncRequest.SourceDeployment,
						"sinkDeployment", msg.Item.StartSyncRequest.SinkDeployment)
				}
			} else if msg.Item.StopSyncRequest != nil {
				err := state.RegisterStopSync(msg.Item.StopSyncRequest)
				if err != nil {
					log.Warn("failed to register stop sync", "err", err,
						"sourceStreamName", msg.Item.StopSyncRequest.SourceStreamName,
						"sourceDeployment", msg.Item.StopSyncRequest.SourceDeployment,
						"sinkDeployment", msg.Item.StopSyncRequest.SinkDeployment)
				}
			}
		}
	}
}

func (ns *natsSync) handleIncomingMessage(ctx context.Context, msg *v1.MessageBatch) error {
	log := slog.With("operation", "handleIncomingMessage", "from", ns.from, "to", ns.to)
	log.Debug("received batch", "msg", msg)

	return nil
}

type WithError[T any] struct {
	Error error
	Item  T
}

type SyncRequests struct {
	PublishedMessage
	ReachedHead      bool // signal that all existing messages have been sent
	StartSyncRequest *v1.StartSyncRequest
	StopSyncRequest  *v1.StopSyncRequest
}

// subscribe to published sync requests.
// Will block if nothing exists.
// Will send an empty SyncRequests with ReachedHead=true when all existing requests have been sent.
func (ns *natsSync) SubscribeToSync(ctx context.Context) (chan WithError[SyncRequests], error) {
	// determine head sequence
	var headSequence uint64
	{
		subCtx, cancel := context.WithCancel(ctx)
		defer cancel()

		// only deliver last to obtain 'head' sequence
		cfg := jetstream.OrderedConsumerConfig{
			DeliverPolicy:  jetstream.DeliverLastPolicy,
			FilterSubjects: ns.getSyncStreamFilterSubjects()}
		ch, err := ns.js.SubscribeOrderered(subCtx, ns.syncStream, cfg)
		if err != nil {
			return nil, errors.Wrap(err, "failed to subscribe to sync stream")
		}

		for msg := range ch {
			headSequence = msg.Item.Sequence
			break
		}

		cancel()
	}

	cfg := jetstream.OrderedConsumerConfig{
		DeliverPolicy:  jetstream.DeliverLastPerSubjectPolicy,
		FilterSubjects: ns.getSyncStreamFilterSubjects()}

	ch, err := ns.js.SubscribeOrderered(ctx, ns.syncStream, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to subscribe to sync stream")
	}

	resultCh := make(chan WithError[SyncRequests])
	go func() {
		defer close(resultCh)

		reachedHead := false
		for msg := range ch {
			if msg.Error != nil {
				resultCh <- WithError[SyncRequests]{Error: msg.Error}
				return
			}

			pm := msg.Item
			result := WithError[SyncRequests]{
				Item: SyncRequests{
					PublishedMessage: pm,
					ReachedHead:      reachedHead}}
			if !slices.Equal(pm.Headers[headerContentType], []string{contentTypeProto}) {
				result.Error = errors.New("invalid content-type")
			} else if len(pm.Data) == 0 {
				result.Error = errors.New("missing header " + headerGrpcMessageType)
			} else {

				msgType := pm.Headers[headerGrpcMessageType]
				switch msgType[0] {
				case startSyncRequestName:
					var msg v1.StartSyncRequest
					err = proto.Unmarshal(pm.Data, &msg)
					if err != nil {
						result.Error = errors.Wrap(err, "failed to unmarshal start sync request")
					} else {
						result.Item.StartSyncRequest = &msg
					}
				case stopSyncRequestName:
					var msg v1.StopSyncRequest
					err = proto.Unmarshal(pm.Data, &msg)
					if err != nil {
						result.Error = errors.Wrap(err, "failed to unmarshal stop sync request")
					} else {
						result.Item.StopSyncRequest = &msg
					}
				default:
					result.Error = errors.Errorf("unknown message type %s", msgType[0])
				}
			}

			select {
			case <-ctx.Done():
				return
			case resultCh <- result:
			}

			// signal that all existing requests have been sent
			if !reachedHead && pm.Sequence >= headSequence {
				m := WithError[SyncRequests]{
					Item: SyncRequests{
						PublishedMessage: pm,
						ReachedHead:      true}}

				select {
				case <-ctx.Done():
					return
				case resultCh <- m:
					reachedHead = true
				}
			}
		}
	}()
	return resultCh, nil
}

// func (ns *NatsSync) SubscribeSyncRequests(ctx context.Context) ([]SyncRequests, error) {

// 	var headSequence uint64
// 	{
// 		subCtx, cancel := context.WithCancel(ctx)
// 		defer cancel()

// 		// only deliver last to obtain 'head' sequence
// 		cfg := jetstream.OrderedConsumerConfig{
// 			DeliverPolicy:  jetstream.DeliverLastPolicy,
// 			FilterSubjects: ns.getSyncStreamFilterSubjects()}
// 		ch, err := ns.js.SubscribeOrderered(subCtx, ns.config.SyncStream, cfg)
// 		if err != nil {
// 			return nil, errors.Wrap(err, "failed to subscribe to sync stream")
// 		}

// 		for msg := range ch {
// 			headSequence = msg.Item.Sequence
// 			break
// 		}

// 		cancel()
// 	}

// 	// subscribe (with last-per-subject) to get all sync requests
// 	// Stop when head sequence is reached
// 	subCtx, cancel := context.WithCancel(ctx)
// 	defer cancel()

// 	var result []SyncRequests
// 	ch, err := ns.SubscribeToSync(subCtx)
// 	if err != nil {
// 		return nil, errors.Wrap(err, "failed to subscribe to sync stream")
// 	}

// 	reachedHead := false
// 	for msg := range ch {
// 		if msg.Error == nil {
// 			result = append(result, msg.Item)
// 		}
// 		if msg.Item.Sequence >= headSequence {
// 			reachedHead = true
// 			break
// 		}
// 	}

// 	if !reachedHead {
// 		return nil, errors.New("failed to reach head sequence")
// 	}

// 	return result, nil
// }

func (ns *natsSync) getSyncStreamFilterSubjects() []string {
	return []string{
		fmt.Sprintf("%s.%s.%s.>", ns.syncStream, ns.from.String(), ns.to.String()), // source
		fmt.Sprintf("%s.%s.%s.>", ns.syncStream, ns.to.String(), ns.from.String()), // sink
	}
}
