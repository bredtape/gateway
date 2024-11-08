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
	"github.com/bredtape/slogging"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

/*
TODO:
* Re-subscribing in *natsSync should have backoff.

*/

const (
	// subscription stream name placeholder used in published messages.
	// The actual stream may have a different name and is configured in the NatsSyncConfig for
	// each deployment
	SyncStreamPlaceholder = "gateway_sync"
)

var (
	startSyncRequestName = string((&v1.StartSyncRequest{}).ProtoReflect().Descriptor().FullName())
	stopSyncRequestName  = string((&v1.StopSyncRequest{}).ProtoReflect().Descriptor().FullName())
)

type NatsSyncConfig struct {
	// 'this' deployment
	Deployment gateway.Deployment

	// indicates if the 'sync' stream originates from this deployment. Exactly one deployment must have this set to true
	IsSyncSource bool

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
	isSyncSourceCount := 0
	if c.IsSyncSource {
		isSyncSourceCount++
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

		if s.IsSyncSource {
			isSyncSourceCount++
		}
		if isSyncSourceCount > 1 {
			return errors.New("more than one deployment has IsSyncSource set to true")
		}
	}

	if isSyncSourceCount == 0 {
		return errors.New("no deployment has IsSyncSource set to true")
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

// nats sync for from and to deployment (bidirectional)
type natsSync struct {
	from, to gateway.Deployment
	// communication settings for 'to'
	cs         CommunicationSettings
	syncStream string

	// rename stream and subject prefix from local name to 'global' name
	streamRenamesForward map[string]string
	// rename stream and subject prefix from 'global' name to local name
	streamRenamesReverse map[string]string
	exchange             Exchange
	js                   *JSConn
	state                *state
	// last sequence per sink subscription
	sinkLastSequence map[SinkSubscriptionKey]SourceSinkSequence

	// active source subscriptions with handle to cancel it
	sourceSubscriptions map[SourceSubscriptionKey]func()

	// channel to signal that a source subscription has been cancelled
	cancelEvent chan SourceSubscriptionKey
}

func StartNatsSync(ctx context.Context, js *JSConn, config NatsSyncConfig) error {
	if err := config.Validate(); err != nil {
		return errors.Wrap(err, "invalid config")
	}

	for d, cs := range config.CommunicationSettings {
		ns := &natsSync{
			from:                 config.Deployment,
			to:                   d,
			cs:                   cs,
			syncStream:           config.SyncStream,
			streamRenamesForward: map[string]string{config.SyncStream: SyncStreamPlaceholder},
			streamRenamesReverse: make(map[string]string),
			exchange:             config.Exchanges[d],
			js:                   js,
			sinkLastSequence:     make(map[SinkSubscriptionKey]SourceSinkSequence),
			sourceSubscriptions:  make(map[SourceSubscriptionKey]func()),
			cancelEvent:          make(chan SourceSubscriptionKey)}
		for k, v := range ns.streamRenamesForward {
			ns.streamRenamesReverse[v] = k
		}
		go ns.outerLoop(ctx)
	}

	return nil
}

func (ns *natsSync) outerLoop(ctx context.Context) {
	r := retry.Must(retry.NewExp(0.4, time.Second, 10*time.Second))

	r.Try(ctx, func() error {
		log := slog.With("from", ns.from, "to", ns.to)

		metricsSyncInitCount.WithLabelValues(ns.labelsFromTo()...).Inc()
		mErr := metricsSyncInitErrorCount.WithLabelValues(ns.labelsFromTo()...)

		ctxInner, cancel := context.WithCancel(ctx)
		err := ns.loop(ctxInner, log)
		cancel()
		if err != nil {
			mErr.Inc()
			log.Error("loop failed, will restart", "err", err)
		}
		return err
	})
}

func (ns *natsSync) loop(ctx context.Context, log *slog.Logger) error {
	log.Log(ctx, slog.LevelDebug-3, "start loop")
	defer log.Log(ctx, slog.LevelDebug-3, "exited loop")
	mSyncHead := metricsSyncHead.WithLabelValues(ns.labelsFromTo()...)
	mSyncHead.Set(0)

	if state, err := newState(ns.from, ns.to, ns.cs, nil); err != nil {
		return errors.Wrap(err, "failed to create state")
	} else {
		ns.state = state
	}

	// register sync stream itself to bootstrap sync
	{
		startSyncReq := &v1.StartSyncRequest{
			SourceStreamName: SyncStreamPlaceholder,
			SourceDeployment: ns.from.String(),
			SinkDeployment:   ns.to.String()}
		// the 'to' deployment is the source
		if ns.cs.IsSyncSource {
			startSyncReq = &v1.StartSyncRequest{
				SourceStreamName: SyncStreamPlaceholder,
				SourceDeployment: ns.to.String(),
				SinkDeployment:   ns.from.String()}
		}

		err := ns.state.RegisterStartSync(startSyncReq)
		if err != nil {
			return errors.Wrap(err, "failed to register start sync for 'sync' itself")
		}
	}

	// subscribe to local sync stream
	syncSubKey := SourceSubscriptionKey{SourceStreamName: SyncStreamPlaceholder}
	syncHeadSequence, err := ns.js.GetLastSequence(ctx, ns.syncStream) // with the real stream name
	if err != nil {
		return errors.Wrapf(err, "failed to get last sequence for sync stream '%s'", ns.syncStream)
	}
	log.Debug("got last sequence for sync stream", "sourceStream", syncSubKey, "syncStream", ns.syncStream, "sequence", syncHeadSequence)

	syncSource := make(chan sourcePublishedMessage)
	ns.sourceStartSubscription(ctx, syncSource, ns.cs.HeartbeatIntervalPrSubscription, SourceSubscription{SourceSubscriptionKey: syncSubKey})

	// start exchange
	realExchangeIncomingCh, err := ns.exchange.StartReceiving(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to start receiving for deployment")
	}

	// exchangeIncomingCh messages are initially ignored, until existing sync requests have been processed
	exchangeIncomingCh := make(<-chan *v1.MessageBatch)
	reachedSyncHead := syncHeadSequence == 0
	flushTimerActive := false
	flushTimer := make(<-chan time.Time) // disabled
	realSourceMessagesCh := make(chan sourcePublishedMessage)
	disabledSourceMessagesCh := make(chan sourcePublishedMessage)
	// messages read from sourceMessagesCh but cannot be delivered due to backoff
	var sourceMessagesNotDelivered []sourcePublishedMessage

	for {
		if reachedSyncHead {
			mSyncHead.Set(1)
			exchangeIncomingCh = realExchangeIncomingCh
		} else {
			log.Debug("waiting for all existing sync requests to be read")
		}

		ns.sinkProcessIncoming(ctx, log)
		ns.sourceStartAndCancelSubscriptions(ctx, log, realSourceMessagesCh)

		// if pending messages/acks, start timer for flush, if not already started.
		if !flushTimerActive && reachedSyncHead {
			stats := ns.state.PendingStats(time.Now())
			if slices.ContainsFunc(stats, func(x int) bool {
				return x > 0
			}) {
				flushTimerActive = true
				flushTimer = time.After(ns.cs.BatchFlushTimeout)
				log.Log(ctx, slog.LevelDebug-3, "started flush timer because of pending", "stats", stats)
			}
		}

		// try to deliver messages that were not delivered due to backoff. There should only be 0..1
		if reachedSyncHead && len(sourceMessagesNotDelivered) > 0 {
			msg := sourceMessagesNotDelivered[0]
			log2 := log.With("sourceSubscriptionKey", msg.SourceSubscriptionKey,
				"lastSequence", msg.LastSequence,
				"count", len(msg.Messages))
			if ns.sourceDeliverFromLocal(ctx, log2, msg) {
				sourceMessagesNotDelivered = sourceMessagesNotDelivered[1:]
			}
		}

		sourceMessagesCh := realSourceMessagesCh
		if len(sourceMessagesNotDelivered) > 0 {
			sourceMessagesCh = disabledSourceMessagesCh
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

		case <-ns.cancelEvent:
			// nop (just to trigger sourceStartSubscriptions)

		// if pending messages/acks and flush timer have fired,
		// then write outgoing batch via Exchange
		case <-flushTimer:
			flushTimerActive = false
			flushTimer = make(<-chan time.Time) // disabled

			metricsBatchWriteTotal.WithLabelValues(ns.labelsFromTo()...).Inc()
			mErr := metricsBatchWriteErrorTotal.WithLabelValues(ns.labelsFromTo()...)

			batch, err := ns.state.CreateMessageBatch(time.Now())
			if err != nil {
				mErr.Inc()
				log.Error("failed to create message batch", "err", err)
				continue
			}
			opCtx, cancel := context.WithTimeout(ctx, ns.cs.ExchangeOperationTimeout)
			err = ns.exchange.Write(opCtx, batch)
			cancel()
			if err != nil {
				mErr.Inc()
				log.Error("failed to write message batch. Ignoring", "err", err)
			} else {
				log.Log(ctx, slog.LevelDebug-3, "wrote message batch")

				report := ns.state.MarkDispatched(batch)
				if !report.IsEmpty() {
					mErr.Inc()
					log.Debug("mark dispatched failed", "report", report)
				}
			}

		// incoming messages from exchange
		case msg, ok := <-exchangeIncomingCh:
			if !ok {
				return errors.New("incoming channel closed")
			}

			metricsHandleIncomingTotal.WithLabelValues(ns.labelsFromTo()...).Inc()
			mErr := metricsHandleIncomingErrorTotal.WithLabelValues(ns.labelsFromTo()...)

			if !ns.matchesThisDeployment(msg) {
				mErr.Inc()
				log.Warn("ignoring batch, not matching this deployment",
					"batchFrom", msg.GetFromDeployment(), "batchTo", msg.GetToDeployment())
				continue
			}

			log.Log(ctx, slog.LevelDebug-3, "received incoming message")
			err := ns.handleIncomingRemoteMessage(msg)
			if err != nil {
				mErr.Inc()
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

			if !ns.sourceDeliverFromLocal(ctx, log2, msg) {
				sourceMessagesNotDelivered = append(sourceMessagesNotDelivered, msg)
			}

		case msg, ok := <-syncSource:
			if !ok {
				return errors.New("sync source channel closed")
			}

			log2 := log.With("sourceSubscriptionKey", msg.SourceSubscriptionKey,
				"lastSequence", msg.LastSequence,
				"count", len(msg.Messages))

			if msg.SourceSubscriptionKey != syncSubKey {
				slogging.Fatal(log2, "unexpected source subscription key")
			}

			ns.handleSyncMessage(log2, msg)

			if !reachedSyncHead && uint64(msg.GetSequenceRange().To) >= syncHeadSequence {
				reachedSyncHead = true
				log.Debug("reached sync head")
			}
		}
	}
}

// source, deliver messages from local state. Returns true if processed, false if backoff
func (ns *natsSync) sourceDeliverFromLocal(ctx context.Context, log2 *slog.Logger, msg sourcePublishedMessage) bool {
	// init labels
	metricLast := metricsSourceDeliveredLastSequence.WithLabelValues(ns.labelsFromToStream(msg.SourceSubscriptionKey.SourceStreamName)...)
	metricsSourceDeliverAttempts.WithLabelValues(ns.labelsFromToStream(msg.SourceSubscriptionKey.SourceStreamName)...).Inc()
	metricErr := metricsSourceDeliverAttemptsError.WithLabelValues(ns.labelsFromToStream(msg.SourceSubscriptionKey.SourceStreamName)...)

	count, err := ns.state.SourceDeliverFromLocal(msg.SourceSubscriptionKey, msg.LastSequence, msg.Messages...)
	metricsSourceAcceptTotal.WithLabelValues(ns.labelsFromToStream(msg.SourceSubscriptionKey.SourceStreamName)...).Add(float64(count))
	if err != nil {
		metricErr.Inc()
		log2.Log(ctx, slog.LevelDebug-3, "failed to deliver message", "err", err,
			"operation", "state/SourceDeliverFromLocal",
			"lastSequence", msg.LastSequence, "count", len(msg.Messages))
		if errors.Is(err, ErrSourceSequenceBroken) {
			log2.Debug("cancelling subscription")
			ns.sourceCancelSubscription(log2, msg.SourceSubscriptionKey)
		} else if errors.Is(err, ErrBackoff) {
			return false
		}
	} else {
		metricLast.Set(float64(msg.GetSequenceRange().To))
		log2.Log(ctx, slog.LevelDebug-3, "delivered message(s) from local source", "acceptedCount", count,
			"operation", "state/SourceDeliverFromLocal")
	}

	return true
}

// source, cancel subscription
func (ns *natsSync) sourceCancelSubscription(log *slog.Logger, key SourceSubscriptionKey) {
	if cancel, exists := ns.sourceSubscriptions[key]; exists {
		cancel()
		delete(ns.sourceSubscriptions, key)
		log.Debug("cancelled subscription", "sourceSubscriptionKey", key)
		go func() {
			ns.cancelEvent <- key
		}()
		metricsSourceDeliveredLastSequence.DeleteLabelValues(ns.labelsFromToStream(key.SourceStreamName)...)
	}
}

func (ns *natsSync) labelsFromToStream(stream string) []string {
	return []string{ns.from.String(), ns.to.String(), stream}
}

func (ns *natsSync) sourceStartAndCancelSubscriptions(ctx context.Context, log *slog.Logger, incoming chan<- sourcePublishedMessage) {
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
		ns.sourceStartSubscription(subCtx, incoming, ns.cs.HeartbeatIntervalPrSubscription, sub)
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

func (m sourcePublishedMessage) GetSequenceRange() RangeInclusive[SourceSequence] {
	r := RangeInclusive[SourceSequence]{From: m.LastSequence, To: m.LastSequence}
	if len(m.Messages) > 0 {
		r.To = SourceSequence(m.Messages[len(m.Messages)-1].Sequence)
	}
	return r
}

// start subscription to source stream.
// Will stop if ctx expires.
// On error the subscription is stopped, and a message is published to the 'incoming' channel with the error
func (ns *natsSync) sourceStartSubscription(ctx context.Context, incoming chan<- sourcePublishedMessage, heartbeatInterval time.Duration, sub SourceSubscription) {
	log := slog.With("operation", "sourceStartSubscription",
		"sourceStreamName", sub.SourceStreamName,
		"deliverPolicy", sub.DeliverPolicy)
	metricsSourceDeliveredLastSequence.WithLabelValues(ns.labelsFromToStream(sub.SourceStreamName)...) // initialize label
	realStream, exists := ns.streamRenamesReverse[sub.SourceStreamName]
	if !exists {
		realStream = sub.SourceStreamName
	}
	if realStream != sub.SourceStreamName {
		log = log.With("realStream", realStream)
	}
	cfg := jetstream.OrderedConsumerConfig{
		DeliverPolicy:  sub.DeliverPolicy,
		OptStartSeq:    uint64(sub.OptStartSeq),
		FilterSubjects: sub.FilterSubjects}
	if cfg.DeliverPolicy == jetstream.DeliverByStartTimePolicy {
		cfg.OptStartTime = &sub.OptStartTime
	}

	lastSequence := SourceSequence(0)
	if sub.DeliverPolicy == jetstream.DeliverByStartSequencePolicy {
		lastSequence = SourceSequence(sub.OptStartSeq)
		log.Debug("starting subscription with start sequence", "sequence", lastSequence)
	} else {
		log.Debug("starting subscription")
	}

	go func() {
		defer log.Debug("subscription was closed")
		heartbeatTimer := time.After(heartbeatInterval)
		lastActivity := time.Time{}
		lastSequenceRelayed := SourceSequence(0)

		ch := ns.js.StartSubscribeOrderered(ctx, realStream, cfg)

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					// publish error and quit
					m := sourcePublishedMessage{
						SourceSubscriptionKey: sub.SourceSubscriptionKey,
						Error:                 errors.New("underlying subscription channel closed")}
					relayMessage(ctx, incoming, m)
					return
				}

				m := sourcePublishedMessage{
					SourceSubscriptionKey: sub.SourceSubscriptionKey,
					LastSequence:          lastSequence,
					Messages:              []*v1.Msg{ns.sourcePublishedMessageToV1(msg)}}

				subject := m.Messages[0].GetSubject()
				if !strings.HasPrefix(subject, sub.SourceStreamName) {
					slogging.Fatal(log, "unexpected subject", "subject", subject, "sourceSubscriptionKey", sub.SourceSubscriptionKey,
						"renamesForward", ns.streamRenamesForward, "renamesReverse", ns.streamRenamesReverse)
				}

				log2 := log.With("lastSequence", lastSequence, "sequence", msg.Sequence)
				log2.Log(ctx, slog.LevelDebug-3, "have message to send")
				if !relayMessage(ctx, incoming, m) {
					return
				}
				log2.Log(ctx, slog.LevelDebug-3, "message was sent")

				lastSequence = SourceSequence(msg.Sequence)
				lastSequenceRelayed = lastSequence
				lastActivity = time.Now()

			case <-heartbeatTimer:
				// there was some activity since last heartbeat, adjust timer
				if time.Since(lastActivity) < heartbeatInterval {
					heartbeatTimer = time.After(heartbeatInterval - time.Since(lastActivity))
					continue
				}

				heartbeatTimer = time.After(heartbeatInterval)

				if lastSequenceRelayed == 0 {
					continue
				}

				heartbeatMsg := sourcePublishedMessage{
					SourceSubscriptionKey: sub.SourceSubscriptionKey,
					LastSequence:          lastSequenceRelayed}

				log.Log(ctx, slog.LevelDebug-3, "have heartbeat to sent", "lastSequenceRelayed", lastSequenceRelayed)
				if !relayMessage(ctx, incoming, heartbeatMsg) {
					return
				}
				log.Log(ctx, slog.LevelDebug-3, "heartbeat was relayed", "lastSequenceRelayed", lastSequenceRelayed)
			}
		}
	}()
}

// try to relay message, unless ctx has expired. Returns true if successful.
func relayMessage[T any](ctx context.Context, ch chan<- T, item T) bool {
	if ctx.Err() != nil {
		return false
	}
	select {
	case <-ctx.Done():
		return false
	case ch <- item:
		return true
	}
}

// write buffered incoming messages to nats stream
func (ns *natsSync) sinkProcessIncoming(ctx context.Context, log *slog.Logger) {
	for key, xs := range ns.state.SinkIncoming {
		skipCount := 0
		for _, msgs := range xs {
			metricsProcessIncomingTotal.WithLabelValues(ns.labelsFromToStream(key.SourceStreamName)...).Inc()
			mErr := metricsProcessIncomingErrorTotal.WithLabelValues(ns.labelsFromToStream(key.SourceStreamName)...)

			opCtx, cancel := context.WithTimeout(ctx, ns.cs.NatsOperationTimeout)
			lastPair, err := ns.getLastSourceSinkSequencePublished(opCtx, key, true)
			cancel()
			if err != nil {
				mErr.Inc()
				log.Error("failed to get last sequence", "err", err, "sourceStreamName", key.SourceStreamName)
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
					mErr.Inc()
					log2.Debug("failed to reject messages. Ignoring", "err", err)
				} else {
					log2.Debug("rejected messages, because sequence is before last published")
				}

				continue
			}

			if lastPair.SourceSequence < r.From {
				if skipCount >= ns.cs.PendingIncomingMessagesPrSubscriptionDeleteThreshold {
					err = ns.state.SinkCommitReject(msgs, lastPair.SourceSequence)
					if err != nil {
						mErr.Inc()
						log2.Debug("failed to reject messages. Ignoring", "err", err)
					} else {
						log2.Log(ctx, slog.LevelDebug-3, "rejected messages, because sequence is after last published")
					}

					continue
				}
				skipCount += len(msgs.GetMessages())

				log2.Log(ctx, slog.LevelDebug-3, "not processing incoming Msgs (because sequence does not match)")
				continue
			}

			ms := msgIncludeFromSequence(lastPair.SourceSequence, msgs.GetMessages())

			opCtx, cancel = context.WithTimeout(ctx, ns.cs.NatsOperationTimeout)
			seq, err := ns.sinkPublishMsgs(opCtx, lastPair, msgs.GetSourceStreamName(), ms)
			cancel()
			if err != nil {
				mErr.Inc()
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
				// reject
				if errors.Is(err, ErrSourceSequenceBroken) {
					err2 := ns.state.SinkCommitReject(msgs, seq.SourceSequence)
					if err2 != nil {
						mErr.Inc()
						log2.Debug("failed to reject messages. Ignoring", "err", err2)
					} else {
						log2.Debug("rejected messages after publish", "err", err)
					}
				} else {
					log2.Debug("failed to commit messages. Ignoring (should self-correct)", "err", err)
				}
			} else {
				log2.Log(ctx, slog.LevelDebug-3, "committed messages")
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
			log.Error("missing header "+headerProtoType, "contentType", pm.Headers[headerContentType])
			continue
		}

		msgType := pm.Headers[headerProtoType]
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

func (ns *natsSync) sinkPublishMsgs(ctx context.Context, lastSequence SourceSinkSequence, stream string, ms []*v1.Msg) (SourceSinkSequence, error) {
	realStream, exists := ns.streamRenamesReverse[stream]
	if !exists {
		realStream = stream
	}
	for _, m := range ms {
		m.Subject = renameSubjectPrefix(ns.streamRenamesReverse, m.GetSubject())
		ack, err := ns.js.PublishRaw(ctx, realStream, lastSequence.SinkSequence, m)
		if err != nil {
			return SourceSinkSequence{}, errors.Wrapf(err,
				"failed to publish message to stream '%s' with subject '%s', sequence %d",
				realStream, m.GetSubject(), m.GetSequence())
		}

		lastSequence.SinkSequence = SinkSequence(ack.Sequence)
		lastSequence.SourceSequence = SourceSequence(m.GetSequence())
	}

	return lastSequence, nil
}

// get last nats sequence published to stream.
// Returns jetstream.ErrStreamNotFound if stream does not exist
// NB: Does not consider FilterSubjects!
func (ns *natsSync) getLastSourceSinkSequencePublished(ctx context.Context, key SinkSubscriptionKey, acceptCached bool) (SourceSinkSequence, error) {
	if acceptCached {
		if seq, exists := ns.sinkLastSequence[key]; exists {
			return seq, nil
		}
	}

	realStream, exists := ns.streamRenamesReverse[key.SourceStreamName]
	if !exists {
		realStream = key.SourceStreamName
	}

	sinkSeq, err := ns.js.GetLastSequence(ctx, realStream)
	if err != nil {
		return SourceSinkSequence{}, errors.Wrapf(err, "failed to get stream info '%s'", realStream)
	}

	if sinkSeq == 0 {
		return SourceSinkSequence{}, nil
	}

	xs, err := ns.js.GetMessageWithSequence(ctx, realStream, sinkSeq)
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

func (ns *natsSync) sourcePublishedMessageToV1(pm PublishedMessage) *v1.Msg {
	m := &v1.Msg{
		Subject:            renameSubjectPrefix(ns.streamRenamesForward, pm.Subject),
		Headers:            make(map[string]string),
		Data:               pm.Data,
		Sequence:           pm.Sequence,
		PublishedTimestamp: timestamppb.New(pm.PublishedTimestamp)}

	for k, vs := range pm.Headers {
		// remove nats headers except MsgID
		if strings.HasPrefix(k, "Nats-") && k != jetstream.MsgIDHeader {
			continue
		}
		m.Headers[k] = strings.Join(vs, ",")
	}
	return m
}

// rename subject prefix if a rename is configured.
// Assume that stream subjects has the stream name as prefix
func renameSubjectPrefix(renames map[string]string, subject string) string {
	xs := strings.SplitN(subject, ".", 2)
	if len(xs) != 2 {
		return subject
	}
	prefix := xs[0]
	other, exists := renames[prefix]
	if !exists {
		return subject
	}

	return other + subject[len(prefix):]
}

func (ns *natsSync) labelsFromTo() []string {
	return []string{string(ns.from), string(ns.to)}
}
