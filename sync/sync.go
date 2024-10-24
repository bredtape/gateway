package sync

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/bredtape/gateway"
	v1 "github.com/bredtape/gateway/sync/v1"
	"github.com/bredtape/retry"
	"github.com/bredtape/set"
	"github.com/bredtape/slogging"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// subscription stream name placeholder used in published messages.
	// The actual stream may have a different name and is configured in the NatsSyncConfig for
	// each deployment
	subscriptionStream = "sync"
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
			from:                config.Deployment,
			to:                  d,
			cs:                  cs,
			syncStream:          config.SyncStream,
			exchange:            config.Exchanges[d],
			js:                  js,
			sinkLastSequence:    make(map[SinkSubscriptionKey]SourceSinkSequence),
			sourceSubscriptions: make(map[SourceSubscriptionKey]func())}
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
	r := retry.Must(retry.NewExp(0.4, time.Second, 10*time.Second))

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
	log.Log(ctx, slog.LevelDebug-3, "start loop")
	defer log.Log(ctx, slog.LevelDebug-3, "exited loop")

	if state, err := newState(ns.from, ns.to, ns.cs, nil); err != nil {
		return errors.Wrap(err, "failed to create state")
	} else {
		ns.state = state
	}

	// register sync stream itself. Do not specify FilterSubjects, as we want to see
	// all messages to determine when head has been reached
	err := ns.state.RegisterStartSync(&v1.StartSyncRequest{
		SourceStreamName: ns.syncStream,
		SourceDeployment: ns.from.String(),
		SinkDeployment:   ns.to.String()})
	if err != nil {
		return errors.Wrap(err, "failed to register start sync for 'sync' itself")
	}

	syncSubKey := SourceSubscriptionKey{SourceStreamName: ns.syncStream}

	syncHeadSequence, err := ns.js.GetLastSequence(ctx, ns.syncStream)
	if err != nil {
		return errors.Wrap(err, "failed to get last sequence for sync stream")
	}
	log.Debug("got last sequence for sync stream", "sequence", syncHeadSequence)

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
			} else {
				log.Log(ctx, slog.LevelDebug-3, "wrote message batch")
			}

		// incoming messages from exchange
		case msg, ok := <-exchangeIncomingCh:
			if !ok {
				return errors.New("incoming channel closed")
			}

			if !ns.matchesThisDeployment(msg) {
				log.Debug("ignoring batch, not matching this deployment",
					"batchFrom", msg.GetFromDeployment(), "batchTo", msg.GetToDeployment())
				continue
			}

			log.Log(ctx, slog.LevelDebug-3, "received incoming message")
			err := ns.handleIncomingRemoteMessage(msg)
			if err != nil {
				log.Error("failed to handle incoming message, ignoring", "err", err)
			}

		case msg, ok := <-sourceMessagesCh:
			if !ok {
				return errors.New("source messages channel closed")
			}

			log2 := log.With("sourceSubscriptionKey", msg.SourceSubscriptionKey,
				"lastSequence", msg.LastSequence,
				"count", len(msg.Messages))
			if msg.Error != nil {
				log2.Warn("failed to receive source message, will cancel subscription (and then later start it again)", "err", msg.Error)
				ns.sourceCancelSubscription(log2, msg.SourceSubscriptionKey)
				continue
			}

			if msg.SourceSubscriptionKey == syncSubKey {
				ns.handleSyncMessage(log2, msg)

				if msg.LastSequence >= SourceSequence(syncHeadSequence) {
					reachedSyncHead = true
					log.Debug("reached sync head", "lastSequence", msg.LastSequence)
				}

				continue
			}

			count, err := ns.state.SourceDeliverFromLocal(msg.SourceSubscriptionKey, msg.LastSequence, msg.Messages...)
			if err != nil {
				log2.Debug("failed to deliver message", "err", err,
					"operation", "state/SourceDeliverFromLocal")
				if errors.Is(err, ErrSourceSequenceBroken) {
					log2.Debug("cancelling subscription")
					ns.sourceCancelSubscription(log2, msg.SourceSubscriptionKey)
				}
			} else {
				log2.Debug("delivered message", "acceptedCount", count)
			}
		}
	}
}

// source, cancel subscription
func (ns *natsSync) sourceCancelSubscription(log *slog.Logger, key SourceSubscriptionKey) {
	if cancel, exists := ns.sourceSubscriptions[key]; exists {
		cancel()
		delete(ns.sourceSubscriptions, key)
		log.Debug("cancelled subscription", "sourceSubscriptionKey", key)
	}
}

func (ns *natsSync) sourceStartSubscriptions(ctx context.Context, log *slog.Logger, incoming chan<- sourcePublishedMessage) {
	actives := getMapKeys(ns.sourceSubscriptions)
	requested := set.NewValues(ns.state.GetSourceLocalSubscriptionKeys()...)

	shouldBeCancelled, _, missing := actives.Diff(requested)
	for key := range shouldBeCancelled {
		ns.sourceCancelSubscription(log, key)
	}

	for key := range missing {
		sub, found := ns.state.GetSourceLocalSubscription(key)
		if !found {
			slogging.Fatal(log, "subscription not found", "key", key)
			continue
		}

		subCtx, cancel := context.WithCancel(ctx)
		err := ns.sourceStartSubscription(subCtx, incoming, ns.cs.HeartbeatIntervalPrSubscription, sub)
		if err != nil {
			cancel()
			log.Error("failed to start subscription", "err", err, "sourceStreamName", sub.SourceStreamName)
			continue
		}

		log.Debug("started subscription",
			"sourceStreamName", sub.SourceStreamName,
			"deliverPolicy", sub.DeliverPolicy,
			"optStartSeq", sub.OptStartSeq)
		ns.sourceSubscriptions[key] = cancel
	}
}

type sourcePublishedMessage struct {
	SourceSubscriptionKey SourceSubscriptionKey
	Error                 error

	// not present on error
	LastSequence SourceSequence
	Messages     []*v1.Msg
}

func (ns *natsSync) sourceStartSubscription(ctx context.Context, incoming chan<- sourcePublishedMessage, heartbeatInterval time.Duration, sub SourceSubscription) error {
	log := slog.With("operation", "sourceStartSubscription", "sourceStreamName", sub.SourceStreamName)
	cfg := jetstream.OrderedConsumerConfig{
		DeliverPolicy:  sub.DeliverPolicy,
		OptStartSeq:    uint64(sub.OptStartSeq),
		OptStartTime:   &sub.OptStartTime,
		FilterSubjects: sub.FilterSubjects}

	lastSequence := SourceSequence(0)
	if sub.DeliverPolicy == jetstream.DeliverByStartSequencePolicy {
		lastSequence = SourceSequence(sub.OptStartSeq)
	}

	ch, err := ns.js.SubscribeOrderered(ctx, sub.SourceStreamName, cfg)
	if err != nil {
		return errors.Wrap(err, "failed to subscribe to stream")
	}

	go func() {
		heartbeatTimer := time.After(heartbeatInterval)
		lastActivity := time.Time{}

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}

				m := sourcePublishedMessage{
					SourceSubscriptionKey: sub.SourceSubscriptionKey,
					Error:                 msg.Error,
					LastSequence:          lastSequence}

				if msg.Error == nil {
					m.Messages = []*v1.Msg{publishedMessageToV1(msg.Item)}
				}

				log2 := log.With("lastSequence", lastSequence, "sequence", msg.Item.Sequence)
				log2.Log(ctx, slog.LevelDebug-3, "have message to sent")
				select {
				case <-ctx.Done():
					return
				case incoming <- m:
					log2.Log(ctx, slog.LevelDebug-3, "message was relayed")
				}

				lastSequence = SourceSequence(msg.Item.Sequence)
				lastActivity = time.Now()

			case <-heartbeatTimer:
				// there was some activity since last heartbeat, adjust timer
				if time.Since(lastActivity) < heartbeatInterval {
					heartbeatTimer = time.After(heartbeatInterval - time.Since(lastActivity))
					continue
				}

				heartbeatMsg := sourcePublishedMessage{
					SourceSubscriptionKey: sub.SourceSubscriptionKey,
					LastSequence:          lastSequence}

				log.Log(ctx, slog.LevelDebug-3, "have heartbeat to sent", "lastSequence", lastSequence)
				select {
				case <-ctx.Done():
					return
				case incoming <- heartbeatMsg:
					log.Log(ctx, slog.LevelDebug-3, "heartbeat was relayed", "lastSequence", lastSequence)
				}
				heartbeatTimer = time.After(heartbeatInterval)
			}
		}
	}()

	return nil
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
				"msgsRange", r.String(),
				"lastPair", lastPair.String())

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

				log2.Log(ctx, slog.LevelDebug-3, "not processing incoming Msgs (because sequence does not match)")
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
			}

			log2.Log(ctx, slog.LevelDebug-3, "published messages")
			ns.sinkLastSequence[key] = seq

			err = ns.state.SinkCommit(msgs)
			if err != nil {
				log2.Debug("failed to commit messages. Ignoring", "err", err)
				panic("failed to commit messages")
			}
		}
	}
}

func (ns *natsSync) handleIncomingRemoteMessage(msg *v1.MessageBatch) error {
	log := slog.With("operation", "handleIncomingRemoteMessage", "from", ns.from, "to", ns.to)
	log.Debug("received batch", "msg", msg)
	now := time.Now()

	if msg.GetToDeployment() != ns.from.String() {
		return fmt.Errorf("unexpected 'to' deployment %s", msg.GetToDeployment())
	}
	if msg.GetFromDeployment() != ns.to.String() {
		return fmt.Errorf("unexpected 'from' deployment %s", msg.GetFromDeployment())
	}

	for _, ack := range msg.Acknowledges {
		wasPending, err := ns.state.SourceHandleSinkAck(now, msg.SentTimestamp.AsTime(), ack)
		if err != nil {
			log.Debug("failed to handle sink ack", "err", err, "ack", ack)
			return errors.Wrap(err, "failed to handle sink ack")
		}
		logMsg := "processed sink ack"
		if !wasPending {
			logMsg = "ignored sink ack"
		}
		log.Log(context.Background(), slog.LevelDebug-3, logMsg,
			"isNegative", ack.IsNegative, "reason", ack.Reason,
			"setId", ack.SetId, "sequenceFrom", ack.SequenceFrom,
			"sequenceTo", ack.SequenceTo)
	}

	for _, m := range msg.ListOfMessages {
		err := ns.state.SinkDeliverFromRemote(now, m)
		if err != nil {
			log.Debug("failed to deliver message", "err", err, "msg", m)
			return errors.Wrap(err, "failed to deliver message")
		}

		key := SinkSubscriptionKey{SourceStreamName: m.GetSourceStreamName()}
		log.Log(context.Background(), slog.LevelDebug-3, "delivered message",
			"setId", m.GetSetId(), "sourceStreamName", m.GetSourceStreamName(),
			"count", len(m.GetMessages()), "sinkIncomingCount", len(ns.state.SinkIncoming[key]))
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

// handle sync message. Errors will be ignored (just logged).
func (ns *natsSync) handleSyncMessage(parentLog *slog.Logger, spm sourcePublishedMessage) {

	for _, pm := range spm.Messages {
		log := parentLog.With("operation", "handleSyncMessage",
			"sourceStreamName", spm.SourceSubscriptionKey.SourceStreamName,
			"lastSequence", spm.LastSequence,
			"sequence", pm.Sequence)
		log.Debug("received sync message")

		if pm.Headers[headerContentType] != contentTypeProto {
			log.Error("invalid content-type", "contentType", pm.Headers[headerContentType])
			continue
		}
		if len(pm.Data) == 0 {
			log.Error("missing header "+headerGrpcMessageType, "contentType", pm.Headers[headerContentType])
			continue
		}

		msgType := pm.Headers[headerGrpcMessageType]
		switch msgType {
		case startSyncRequestName:
			var msg v1.StartSyncRequest
			err := proto.Unmarshal(pm.Data, &msg)
			if err != nil {
				log.Error("failed to unmarshal start sync request", "err", err)
				continue
			}
			err = ns.state.RegisterStartSync(&msg)
			if err != nil {
				log.Error("failed to register start sync", "err", err)
				continue
			}
			log.Debug("registered start sync request",
				"source", msg.GetSourceDeployment(), "sink", msg.GetSinkDeployment(), "sourceStream", msg.GetSourceStreamName())

		case stopSyncRequestName:
			var msg v1.StopSyncRequest
			err := proto.Unmarshal(pm.Data, &msg)
			if err != nil {
				log.Error("failed to unmarshal stop sync request", "err", err)
				continue
			}
			err = ns.state.RegisterStopSync(&msg)
			if err != nil {
				log.Error("failed to register stop sync", "err", err)
				continue
			}
			log.Debug("registered stop sync request",
				"source", msg.GetSourceDeployment(), "sink", msg.GetSinkDeployment(), "sourceStream", msg.GetSourceStreamName())

		default:
			log.Warn("unknown message type", "type", msgType[0])
		}
	}
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

func (ns *natsSync) matchesThisDeployment(b *v1.MessageBatch) bool {
	return b.GetToDeployment() == ns.from.String() && b.GetFromDeployment() == ns.to.String()
}

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
		if SourceSequence(x.GetSequence()) > minimum {
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

func (s SourceSinkSequence) String() string {
	return fmt.Sprintf("[source %d, sink %d]", s.SourceSequence, s.SinkSequence)
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
