package sync

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/bredtape/gateway"
	v1 "github.com/bredtape/gateway/sync/v1"
	"github.com/bredtape/retry"
	"github.com/bredtape/set"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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
		return errors.New("syncStream empty")
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
			from:             config.Deployment,
			to:               d,
			cs:               cs,
			syncStream:       config.SyncStream,
			exchange:         config.Exchanges[d],
			js:               js,
			sinkLastSequence: make(map[SinkSubscriptionKey]SourceSinkSequence)}
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
	js         *JSConn
	state      *state
	// last sequence per sink subscription
	sinkLastSequence map[SinkSubscriptionKey]SourceSinkSequence

	// active source subscriptions with handle to cancel it
	sourceSubscriptions map[SourceSubscriptionKey]func()
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
	if state, err := newState(ns.from, ns.to, ns.cs, nil); err != nil {
		return errors.Wrap(err, "failed to create state")
	} else {
		ns.state = state
	}

	syncRequestsCh, err := ns.SubscribeToSync(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to subscribe to sync stream")
	}

	realIncomingCh, err := ns.exchange.StartReceiving(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to start receiving for deployment")
	}

	// exchangeIncomingCh messages are initially ignored until existing sync requests have been processed
	exchangeIncomingCh := make(<-chan *v1.MessageBatch)
	reachedSyncHead := false
	flushTimerActive := false
	flushTimer := make(<-chan time.Time) // disabled
	sourceMessagesCh := make(chan sourcePublishedMessage)

	for {
		if reachedSyncHead {
			if ns.state.HasSubscriptions() {
				exchangeIncomingCh = realIncomingCh
			} else {
				log.Info("no subscriptions, waiting for sync requests")
			}
		}

		ns.sinkProcessIncoming(ctx, log)
		ns.sourceStartSubscriptions(ctx, log, sourceMessagesCh)

		// if pending messages/acks, start timer for flush, if not already started.
		if !flushTimerActive {
			stats := ns.state.PendingStats(time.Now())
			if stats[0] > 0 || stats[1] > 0 {
				flushTimerActive = true
				flushTimer = time.After(ns.cs.BatchFlushTimeout)
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
					 future messages should not be received until the backoff is lifted.

		*/

		case <-ctx.Done():
		// if pending messages/acks and flush timer have fired,
		// then write outgoing batch via Exchange
		case <-flushTimer:
			flushTimerActive = false
			batch, err := ns.state.CreateMessageBatch(time.Now())
			if err != nil {
				log.Error("failed to create message batch", "err", err)
				continue
			}
			opCtx, cancel := context.WithTimeout(ctx, ns.cs.ExchangeOperationTimeout)
			err = ns.exchange.Write(opCtx, batch)
			cancel()
			if err != nil {
				log.Error("failed to write message batch. Ignoring", "err", err)
			}

		// incoming messages from exchange
		case msg, ok := <-exchangeIncomingCh:
			if !ok {
				return errors.New("incoming channel closed")
			}

			err := ns.handleIncomingRemoteMessage(msg)
			log.Error("failed to handle incoming message, ignoring", "err", err)

		case msg, ok := <-sourceMessagesCh:
			if !ok {
				return errors.New("source messages channel closed")
			}

			log2 := log.With("sourceSubscriptionKey", msg.SourceSubscriptionKey)

			if msg.Error != nil {
				log2.Warn("failed to receive source message, will cancel subscription", "err", msg.Error)
				ns.sourceCancelSubscription(msg.SourceSubscriptionKey)
				continue
			}

			count, err := ns.state.SourceDeliverFromLocal(msg.SourceSubscriptionKey, msg.LastSequence, msg.Msg)
			if err != nil {
				log2.Debug("failed to deliver message", "err", err)
				if errors.Is(err, ErrSourceSequenceBroken) {
					log2.Debug("cancelling subscription")
					ns.sourceCancelSubscription(msg.SourceSubscriptionKey)
				}
			} else {
				log2.Debug("delivered message", "count", count)
			}

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
				err := ns.state.RegisterStartSync(msg.Item.StartSyncRequest)
				if err != nil {
					log.Warn("failed to register start sync", "err", err,
						"sourceStreamName", msg.Item.StartSyncRequest.SourceStreamName,
						"sourceDeployment", msg.Item.StartSyncRequest.SourceDeployment,
						"sinkDeployment", msg.Item.StartSyncRequest.SinkDeployment)
				}
			} else if msg.Item.StopSyncRequest != nil {
				err := ns.state.RegisterStopSync(msg.Item.StopSyncRequest)
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

func (ns *natsSync) sourceStartSubscriptions(ctx context.Context, log *slog.Logger, incoming chan<- sourcePublishedMessage) {
	actives := getMapKeys(ns.sourceSubscriptions)
	requested := set.NewValues(ns.state.GetSourceLocalSubscriptionKeys()...)

	shouldBeCancelled, _, missing := actives.Diff(requested)
	for key := range shouldBeCancelled {
		ns.sourceCancelSubscription(key)
	}

	for key := range missing {
		sub, found := ns.state.GetSourceLocalSubscriptions(key)
		if !found {
			log.Error("subscription not found", "key", key)
			continue
		}

		subCtx, cancel := context.WithCancel(ctx)
		err := ns.sourceStartSubscription(subCtx, incoming, sub)
		if err != nil {
			cancel()
			log.Error("failed to start subscription", "err", err,
				"sourceStreamName", sub.SourceStreamName)
			continue
		}

		ns.sourceSubscriptions[key] = cancel
	}
}

type sourcePublishedMessage struct {
	SourceSubscriptionKey SourceSubscriptionKey
	Error                 error

	// not present on error
	LastSequence SourceSequence
	Msg          *v1.Msg
}

func (ns *natsSync) sourceStartSubscription(ctx context.Context, incoming chan<- sourcePublishedMessage, sub SourceSubscription) error {
	cfg := jetstream.OrderedConsumerConfig{
		DeliverPolicy:  sub.DeliverPolicy,
		OptStartSeq:    sub.OptStartSeq,
		OptStartTime:   &sub.OptStartTime,
		FilterSubjects: []string{sub.SourceStreamName}}

	lastSequence := SourceSequence(0)
	if sub.DeliverPolicy == jetstream.DeliverByStartSequencePolicy {
		lastSequence = SourceSequence(sub.OptStartSeq)
	}

	ch, err := ns.js.SubscribeOrderered(ctx, sub.SourceStreamName, cfg)
	if err != nil {
		return errors.Wrap(err, "failed to subscribe to stream")
	}

	go func() {
		for msg := range ch {
			m := sourcePublishedMessage{
				SourceSubscriptionKey: sub.SourceSubscriptionKey,
				Error:                 msg.Error,
				LastSequence:          lastSequence}

			if msg.Error == nil {
				m.Msg = publishedMessageToV1(msg.Item)
			}

			select {
			case <-ctx.Done():
				return
			case incoming <- m:
			}

			lastSequence = SourceSequence(msg.Item.Sequence)
		}
	}()

	return nil
}

// source, cancel subscription
func (ns *natsSync) sourceCancelSubscription(key SourceSubscriptionKey) {
	if cancel, exists := ns.sourceSubscriptions[key]; exists {
		cancel()
	}
	delete(ns.sourceSubscriptions, key)
}

// write buffered incoming messages to nats stream
func (ns *natsSync) sinkProcessIncoming(ctx context.Context, log *slog.Logger) {
	for key, xs := range ns.state.SinkIncoming {
		skipCount := 0
		for _, msgs := range xs {
			opCtx, cancel := context.WithTimeout(ctx, ns.cs.NatsOperationTimeout)
			lastPair, err := ns.getLastSourceSinkSequencePublished(opCtx, key, true)
			cancel()
			if err != nil {
				log.Error("failed to get last sequence", "err", err)
				continue
			}

			r := getMsgsRange(msgs)

			log2 := log.With(
				"sourceStreamName", msgs.GetSourceStreamName(),
				"sourceSequence", lastPair.SourceSequence,
				"sinkSequence", lastPair.SinkSequence,
				"msgsRange", r.String())

			if lastPair.SourceSequence > r.To {
				err := ns.state.SinkCommitReject(msgs, lastPair.SourceSequence)
				if err != nil {
					log2.Debug("failed to reject messages. Ignoring", "err", err)
				} else {
					log2.Debug("rejected messages")
				}

				continue
			}

			if lastPair.SourceSequence < r.From {

				if skipCount >= ns.cs.PendingIncomingMessagesPrSubscriptionDeleteThreshold {
					err = ns.state.SinkCommitReject(msgs, lastPair.SourceSequence)
					if err != nil {
						log2.Debug("failed to reject messages. Ignoring", "err", err)
					} else {
						log2.Log(ctx, slog.LevelDebug-3, "rejected messages")
					}

					continue
				}
				skipCount += len(msgs.GetMessages())
				continue
			}

			ms := msgIncludeFromSequence(lastPair.SourceSequence, msgs.GetMessages())

			opCtx, cancel = context.WithTimeout(ctx, ns.cs.NatsOperationTimeout)
			seq, err := ns.publishMsgs(opCtx, lastPair, msgs.GetSourceStreamName(), ms)
			cancel()
			if err != nil {
				if isJetstreamConcurrencyError(err) {
					log2.Debug("jetstream concurrency error, removing cached value of last sequence. Will retry, later", "err", err)
					delete(ns.sinkLastSequence, key)
					continue
				}

				log2.Error("failed to publish messages. Will retry, later", "err", err)
				continue
			} else {
				log2.Log(ctx, slog.LevelDebug-3, "published messages")
				ns.sinkLastSequence[key] = seq

				err = ns.state.SinkCommit(msgs)
				if err != nil {

				}
			}
		}
	}
}

func (ns *natsSync) handleIncomingRemoteMessage(msg *v1.MessageBatch) error {
	log := slog.With("operation", "handleIncomingRemoteMessage", "from", ns.from, "to", ns.to)
	log.Debug("received batch", "msg", msg)
	now := time.Now()

	for _, ack := range msg.Acknowledges {
		err := ns.state.SourceHandleSinkAck(now, msg.SentTimestamp.AsTime(), ack)
		if err != nil {
			log.Debug("failed to handle sink ack", "err", err, "ack", ack)
			return errors.Wrap(err, "failed to handle sink ack")
		}
	}

	for _, m := range msg.ListOfMessages {
		err := ns.state.SinkDeliverFromRemote(now, m)
		if err != nil {
			log.Debug("failed to deliver message", "err", err, "msg", m)
			return errors.Wrap(err, "failed to deliver message")
		}
	}

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

func (ns *natsSync) getSyncStreamFilterSubjects() []string {
	return []string{
		// source
		fmt.Sprintf("%s.%s.%s.>", ns.syncStream, ns.from.String(), ns.to.String()),
		// sink
		fmt.Sprintf("%s.%s.%s.>", ns.syncStream, ns.to.String(), ns.from.String())}
}

func (ns *natsSync) publishMsgs(ctx context.Context, lastSequence SourceSinkSequence, stream string, ms []*v1.Msg) (SourceSinkSequence, error) {
	for _, m := range ms {
		ack, err := ns.js.PublishRaw(ctx, stream, lastSequence.SinkSequence, m)
		if err != nil {
			return SourceSinkSequence{}, errors.Wrapf(err, "failed to publish message with subject %s, sequence %d",
				m.GetSubject(), m.GetSequence())
		}

		lastSequence.SinkSequence = SinkSequence(ack.Sequence)
		lastSequence.SourceSequence = SourceSequence(m.GetSequence())
	}

	return lastSequence, nil
}

// get last nats sequence published to stream.
// Returns jetstream.ErrStreamNotFound if stream does not exist
func (ns *natsSync) getLastSourceSinkSequencePublished(ctx context.Context, key SinkSubscriptionKey, acceptCached bool) (SourceSinkSequence, error) {
	if acceptCached {
		if seq, exists := ns.sinkLastSequence[key]; exists {
			return seq, nil
		}
	}

	sinkSeq, err := ns.js.GetLastSequence(ctx, key.SourceStreamName)
	if err != nil {
		return SourceSinkSequence{}, errors.Wrap(err, "failed to get stream info")
	}

	if sinkSeq == 0 {
		return SourceSinkSequence{}, nil
	}

	xs, err := ns.js.GetMessageWithSequence(ctx, key.SourceStreamName, sinkSeq)
	if err != nil {
		return SourceSinkSequence{}, errors.Wrap(err, "failed to get message with sequence")
	}

	if len(xs) == 0 {
		return SourceSinkSequence{}, errors.New("failed to get the last message")
	}

	hs, exists := xs[0].Headers[headerSourceSequence]
	if !exists {
		return SourceSinkSequence{}, errors.New("source sequence header missing")
	}

	sourceSeq, err := strconv.ParseUint(hs[0], 10, 64)
	if err != nil {
		return SourceSinkSequence{}, errors.Wrap(err, "failed to parse source sequence")
	}

	return SourceSinkSequence{
		SourceSequence: SourceSequence(sourceSeq),
		SinkSequence:   SinkSequence(sinkSeq)}, nil
}

// func (ns *natsSync) getMatchingSourceSequence(ctx context.Context, sub *v1.Subscription) SinkSubscriptionKey {

// }

func getMsgsRange(m *v1.Msgs) RangeInclusive[SourceSequence] {
	r := RangeInclusive[SourceSequence]{
		From: SourceSequence(m.GetLastSequence()),
		To:   SourceSequence(m.GetLastSequence())}
	if len(m.GetMessages()) > 0 {
		r.To = SourceSequence(m.GetMessages()[len(m.GetMessages())-1].GetSequence())
	}
	return r
}

func msgIncludeFromSequence(minimum SourceSequence, xs []*v1.Msg) []*v1.Msg {
	// TODO: Optimize search for minimum
	for i, x := range xs {
		if SourceSequence(x.GetSequence()) >= minimum {
			return xs[i:]
		}
	}

	return nil
}

// matching source and sink sequence. Used when publishing messages at the sink
type SourceSinkSequence struct {
	// sequence number at the source
	SourceSequence SourceSequence

	// sequence number at the sink, matching the source message
	SinkSequence SinkSequence
}

type SourceSequence uint64
type SinkSequence uint64

func isJetstreamConcurrencyError(err error) bool {
	if err == nil {
		return false
	}

	var e *jetstream.APIError
	return errors.As(err, &e) && e.ErrorCode == jetstream.JSErrCodeStreamWrongLastSequence
}

func getMapKeys[K comparable, V any](m map[K]V) set.Set[K] {
	keys := set.New[K](len(m))
	for k := range m {
		keys.Add(k)
	}
	return keys
}

func publishedMessageToV1(pm PublishedMessage) *v1.Msg {
	m := &v1.Msg{
		Subject:            pm.Subject,
		Headers:            make(map[string]string),
		Data:               pm.Data,
		Sequence:           pm.Sequence,
		PublishedTimestamp: timestamppb.New(pm.PublishedTimestamp)}

	for k, vs := range pm.Headers {
		m.Headers[k] = strings.Join(vs, ",")
	}
	return m
}
