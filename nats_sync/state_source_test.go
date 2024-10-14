package nats_sync

import (
	"testing"
	"time"

	"github.com/bredtape/gateway"
	v1 "github.com/bredtape/gateway/nats_sync/v1"
	"github.com/bredtape/retry"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	testDefaultComm = CommunicationSettings{
		AckTimeoutPrSubscription:                 1 * time.Second,
		NakBackoffPrSubscription:                 retry.Must(retry.NewExp(0.2, 1*time.Second, 5*time.Second)),
		FlushIntervalPrSubscription:              5 * time.Millisecond,
		HeartbeatIntervalPrSubscription:          time.Minute,
		MaxPendingAcksPrSubscription:             10,
		MaxPendingIncomingMessagesPrSubscription: 10,
		MaxAccumulatedPayloadSizeBytes:           2 << 10}
)

func TestStateSourceBasic(t *testing.T) {
	Convey("init state", t, func() {
		source := gateway.Deployment("xx")
		target := gateway.Deployment("yy")
		s, err := newState(source, map[gateway.Deployment]CommunicationSettings{
			target: testDefaultComm})
		So(err, ShouldBeNil)

		msg1 := &v1.Msg{
			Subject:          "x.y.z",
			Data:             []byte("123"),
			Sequence:         2,
			PublishTimestamp: timestamppb.Now()}
		msg2 := &v1.Msg{
			Subject:          "x.y.z",
			Data:             []byte("123"),
			Sequence:         4,
			PublishTimestamp: timestamppb.Now()}

		Convey("register subscription, with this deployment as the source", func() {
			req := &v1.StartSyncRequest{
				SourceDeployment: source.String(),
				ReplyDeployment:  source.String(),
				TargetDeployment: target.String(),
				SourceStreamName: "stream1",
				TargetStreamName: "stream2",
				ConsumerConfig:   &v1.ConsumerConfig{DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL}}
			err := s.RegisterSubscription(req)
			So(err, ShouldBeNil)

			key := SourceSubscriptionKey{
				TargetDeployment: target,
				SourceStreamName: "stream1"}

			Convey("should have source-subscription to deliver last", func() {
				So(s.SourceLocalSubscriptions, ShouldContainKey, key)
				So(s.SourceLocalSubscriptions[key], ShouldResemble, SourceSubscription{
					SourceSubscriptionKey: key,
					DeliverPolicy:         jetstream.DeliverLastPolicy})
			})

			Convey("deliver last msg for stream1", func() {
				_, err := s.SourceDeliverFromLocal(key, 0, msg1)
				So(err, ShouldBeNil)

				Convey("create batch", func() {
					t0 := time.Now()
					batch1, err := s.CreateMessageBatch(t0, target)
					So(err, ShouldBeNil)

					Convey("should have outgoing messages for stream1", func() {
						So(batch1.ListOfMessages, ShouldHaveLength, 1)

						ms := batch1.ListOfMessages[0]
						Convey("with source stream", func() {
							So(ms.SourceStreamName, ShouldEqual, "stream1")
						})
						Convey("with target deployment", func() {
							So(ms.TargetDeployment, ShouldEqual, "yy")
						})
						Convey("with consumer config, deliver all", func() {
							So(ms.ConsumerConfig.DeliverPolicy, ShouldEqual, v1.DeliverPolicy_DELIVER_POLICY_ALL)
						})
					})

					Convey("mark batch1 dispatched as the first", func() {
						t1 := time.Now()
						report := s.MarkDispatched(batch1)
						So(report.IsEmpty(), ShouldBeTrue)

						Convey("create another batch", func() {
							batch2, errs := s.CreateMessageBatch(t1, target)
							So(errs, ShouldBeNil)

							Convey("should be empty", func() {
								So(batch2, ShouldBeNil)
							})
						})

						Convey("HandleTargetAck with ack for stream1, also at t1", func() {
							ack1 := &v1.Acknowledge{
								SetId:            batch1.ListOfMessages[0].SetId,
								SourceStreamName: "stream1",
								SequenceFrom:     0,
								SequenceTo:       2}

							err := s.SourceHandleTargetAck(t1, t1, target, ack1)
							So(err, ShouldBeNil)

							Convey("should have updated Subscriptions, to be deliver from sequence 2", func() {
								So(s.SourceLocalSubscriptions[key], ShouldResemble,
									SourceSubscription{
										SourceSubscriptionKey: key,
										DeliverPolicy:         jetstream.DeliverByStartSequencePolicy,
										OptStartSeq:           2})
							})

							Convey("deliver msg2, after ack1 is received", func() {
								count, err := s.SourceDeliverFromLocal(key, 2, msg2)
								So(err, ShouldBeNil)
								So(count, ShouldEqual, 1)

								Convey("create batch2", func() {
									batch2, errs := s.CreateMessageBatch(t1, target)
									So(errs, ShouldBeNil)
									So(batch2, ShouldNotBeNil)

									Convey("mark batch2 dispatched", func() {
										report := s.MarkDispatched(batch2)
										So(report.IsEmpty(), ShouldBeTrue)
									})
								})
							})
						})

						Convey("HandleTargetAck, with SetID _not_ matching", func() {
							ack2 := &v1.Acknowledge{
								SetId:            NewSetID().String(),
								SourceStreamName: "stream1",
								SequenceFrom:     0,
								SequenceTo:       2}

							err := s.SourceHandleTargetAck(t1, t1, target, ack2)
							So(err, ShouldBeNil)

							Convey("should not have updated Subscriptions", func() {
								So(s.SourceLocalSubscriptions[key].DeliverPolicy, ShouldEqual, jetstream.DeliverLastPolicy)
							})
						})

						Convey("deliver local msg2, before ack1 is received", func() {
							count, err := s.SourceDeliverFromLocal(key, 2, msg2)
							So(err, ShouldBeNil)
							So(count, ShouldEqual, 1)

							t2 := time.Now()
							batch2, errs := s.CreateMessageBatch(t2, target)
							So(errs, ShouldBeNil)
							So(batch2, ShouldNotBeNil)

							Convey("mark batch2 dispatched", func() {
								report := s.MarkDispatched(batch2)
								So(report.IsEmpty(), ShouldBeTrue)

								Convey("HandleTargetAck ack1", func() {
									ack1 := &v1.Acknowledge{
										SetId:            batch1.ListOfMessages[0].SetId,
										SourceStreamName: "stream1",
										SequenceFrom:     0,
										SequenceTo:       2}

									err := s.SourceHandleTargetAck(t1, t1, target, ack1)
									So(err, ShouldBeNil)

									Convey("should have updated Subscriptions, to be deliver from sequence 2", func() {
										So(s.SourceLocalSubscriptions[key], ShouldResemble,
											SourceSubscription{
												SourceSubscriptionKey: key,
												DeliverPolicy:         jetstream.DeliverByStartSequencePolicy,
												OptStartSeq:           2})
									})

									Convey("HandleTargetAck ack2", func() {
										ack2 := &v1.Acknowledge{
											SetId:            batch2.ListOfMessages[0].SetId,
											SourceStreamName: "stream1",
											SequenceFrom:     2,
											SequenceTo:       4}

										err := s.SourceHandleTargetAck(t1, t1, target, ack2)
										So(err, ShouldBeNil)

										Convey("should have updated Subscriptions, to be deliver from sequence 4", func() {
											So(s.SourceLocalSubscriptions[key], ShouldResemble,
												SourceSubscription{
													SourceSubscriptionKey: key,
													DeliverPolicy:         jetstream.DeliverByStartSequencePolicy,
													OptStartSeq:           4})
										})
									})
								})

								Convey("HandleTargetAck ack2", func() {
									ack2 := &v1.Acknowledge{
										SetId:            batch2.ListOfMessages[0].SetId,
										SourceStreamName: "stream1",
										SequenceFrom:     2,
										SequenceTo:       4}

									err := s.SourceHandleTargetAck(t1, t1, target, ack2)
									So(err, ShouldBeNil)

									Convey("should have _not_ have updated Subscriptions", func() {
										So(s.SourceLocalSubscriptions[key].DeliverPolicy, ShouldEqual, jetstream.DeliverLastPolicy)
									})
								})
							})
						})
					})
				})
			})
		})
	})
}

func TestStateSourceBackoff(t *testing.T) {
	Convey("init state", t, func() {
		source := gateway.Deployment("xx")
		target := gateway.Deployment("yy")

		maxPending := testDefaultComm.MaxPendingAcksPrSubscription
		s, err := newState(source, map[gateway.Deployment]CommunicationSettings{target: testDefaultComm})
		So(err, ShouldBeNil)

		Convey("assumed max pending is 10", func() {
			So(maxPending, ShouldEqual, 10)
		})

		Convey("register subscription, with this deployment as the source", func() {
			req := &v1.StartSyncRequest{
				SourceDeployment: source.String(),
				ReplyDeployment:  source.String(),
				TargetDeployment: target.String(),
				SourceStreamName: "stream1",
				TargetStreamName: "stream2",
				ConsumerConfig:   &v1.ConsumerConfig{DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL}}
			err := s.RegisterSubscription(req)
			So(err, ShouldBeNil)

			key := SourceSubscriptionKey{
				TargetDeployment: target,
				SourceStreamName: "stream1"}

			Convey("deliver 10 messages for stream1", func() {
				msgs := make([]*v1.Msg, 0)
				for i := 0; i < maxPending; i++ {
					msg := &v1.Msg{
						Subject:          "x.y.z",
						Data:             []byte("123"),
						Sequence:         uint64(i + 1),
						PublishTimestamp: timestamppb.Now()}
					msgs = append(msgs, msg)
				}

				count, err := s.SourceDeliverFromLocal(key, 0, msgs...)
				So(err, ShouldBeNil)
				So(count, ShouldEqual, 10)

				Convey("should have 10 outgoing messages", func() {
					So(s.PendingStats(target), ShouldResemble, []int{maxPending, 0})
				})

				Convey("deliver 1 more message for stream1", func() {
					msg := &v1.Msg{
						Sequence: uint64(maxPending + 1),
					}
					count, err := s.SourceDeliverFromLocal(key, uint64(maxPending), msg)

					Convey("should have backoff error", func() {
						So(err, ShouldNotBeNil)
						Printf("err: %v\n", err)
						So(errors.Is(err, ErrBackoff), ShouldBeTrue)
					})
					Convey("should not have accepted any messages", func() {
						So(count, ShouldEqual, 0)
					})
					Convey("should still have 10 outgoing messages", func() {
						So(s.PendingStats(target), ShouldResemble, []int{maxPending, 0})
					})
				})

				Convey("deliver message, restarting subscription", func() {
					msg := &v1.Msg{
						Sequence: 2}
					count, err := s.SourceDeliverFromLocal(key, 0, msg)
					So(count, ShouldEqual, 1)

					Convey("should accept message", func() {
						So(err, ShouldBeNil)
					})

					Convey("should have 1 outgoing messages", func() {
						So(s.PendingStats(target), ShouldResemble, []int{1, 0})
					})
				})

				Convey("create batch and dispatch", func() {
					b, err := s.CreateMessageBatch(time.Now(), target)
					So(err, ShouldBeNil)

					report := s.MarkDispatched(b)
					So(report.IsEmpty(), ShouldBeTrue)

					Convey("should have 0 outgoing messages", func() {
						So(s.PendingStats(target), ShouldResemble, []int{0, 0})
					})

					Convey("deliver 1 more message for stream1", func() {
						msg := &v1.Msg{
							Sequence: uint64(maxPending + 1),
						}
						count, err := s.SourceDeliverFromLocal(key, uint64(maxPending), msg)

						Convey("should have backoff error", func() {
							So(err, ShouldNotBeNil)
							Printf("err: %v", err)
							So(errors.Is(err, ErrBackoff), ShouldBeTrue)
						})
						Convey("should not have accepted any messages", func() {
							So(count, ShouldEqual, 0)
						})
					})
				})
			})

			Convey("deliver 8 messages for stream1", func() {
				msgs := make([]*v1.Msg, 0)
				for i := 0; i < 8; i++ {
					msg := &v1.Msg{
						Subject:          "x.y.z",
						Data:             []byte("123"),
						Sequence:         uint64(i + 1),
						PublishTimestamp: timestamppb.Now()}
					msgs = append(msgs, msg)
				}

				count, err := s.SourceDeliverFromLocal(key, 0, msgs...)
				So(err, ShouldBeNil)
				So(count, ShouldEqual, 8)

				Convey("should have 8 outgoing messages", func() {
					So(s.PendingStats(target), ShouldResemble, []int{8, 0})
				})

				Convey("deliver 4 messages", func() {
					msgs := make([]*v1.Msg, 0)
					for i := 0; i < 4; i++ {
						msg := &v1.Msg{
							Subject:          "x.y.z",
							Data:             []byte("123"),
							Sequence:         uint64(i + 9), // start from 9
							PublishTimestamp: timestamppb.Now()}
						msgs = append(msgs, msg)
					}

					count, err := s.SourceDeliverFromLocal(key, 8, msgs...)
					Convey("should have backoff error", func() {
						So(err, ShouldNotBeNil)
						So(errors.Is(err, ErrBackoff), ShouldBeTrue)
					})
					Convey("should have accepted 2 messages", func() {
						So(count, ShouldEqual, 2)
					})
					Convey("should have 10 outgoing messages", func() {
						So(s.PendingStats(target), ShouldResemble, []int{maxPending, 0})
					})
				})
			})
		})
	})
}

func TestStateSourceBatchOverlap(t *testing.T) {

}
