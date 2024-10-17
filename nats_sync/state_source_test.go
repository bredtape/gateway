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
		AckRetryPrSubscription:                   retry.Must(retry.NewSeq(0, 2*time.Second, 5*time.Second)),
		HeartbeatIntervalPrSubscription:          time.Minute,
		MaxPendingAcksPrSubscription:             10,
		MaxPendingIncomingMessagesPrSubscription: 10,
		MaxAccumulatedPayloadSizeBytes:           1 << 10}
)

func TestStateSourceBasic(t *testing.T) {
	Convey("init state", t, func() {
		from := gateway.Deployment("xx")
		to := gateway.Deployment("yy")
		s, err := newState(from, to, testDefaultComm, nil)
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
				SourceDeployment: from.String(),
				SinkDeployment:   to.String(),
				SourceStreamName: "stream1",
				SinkStreamName:   "stream2",
				ConsumerConfig: &v1.ConsumerConfig{
					DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL}}
			err := s.RegisterStartSync(req)
			So(err, ShouldBeNil)

			key := SourceSubscriptionKey{SourceStreamName: "stream1"}

			Convey("should have source-subscription to deliver all", func() {
				sub, exists := s.GetSourceLocalSubscriptions(key)
				So(exists, ShouldBeTrue)
				So(sub.DeliverPolicy, ShouldEqual, jetstream.DeliverAllPolicy)
			})

			Convey("deliver first msg for stream1", func() {
				count, err := s.SourceDeliverFromLocal(key, 0, msg1)
				So(err, ShouldBeNil)
				So(count, ShouldEqual, 1)

				Convey("should have updated Subscriptions", func() {
					sub, exists := s.GetSourceLocalSubscriptions(key)
					So(exists, ShouldBeTrue)
					So(sub.DeliverPolicy, ShouldEqual, jetstream.DeliverByStartSequencePolicy)
					So(sub.OptStartSeq, ShouldEqual, 2)
				})

				Convey("create batch", func() {
					t0 := time.Now()
					batch1, err := s.CreateMessageBatch(t0)
					So(err, ShouldBeNil)

					Convey("should have outgoing messages for stream1", func() {
						So(batch1.ListOfMessages, ShouldHaveLength, 1)

						ms := batch1.ListOfMessages[0]
						Convey("with source stream", func() {
							So(ms.SourceStreamName, ShouldEqual, "stream1")
						})
						Convey("with target deployment", func() {
							So(ms.SinkDeployment, ShouldEqual, "yy")
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
							batch2, errs := s.CreateMessageBatch(t1)
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

							err := s.SourceHandleSinkAck(t1, t1, to, ack1)
							So(err, ShouldBeNil)

							Convey("should have updated Subscriptions, to be deliver from sequence 2", func() {
								sub, exists := s.GetSourceLocalSubscriptions(key)
								So(exists, ShouldBeTrue)
								So(sub.DeliverPolicy, ShouldEqual, jetstream.DeliverByStartSequencePolicy)
								So(sub.OptStartSeq, ShouldEqual, 2)
							})

							Convey("deliver msg2, after ack1 is received", func() {
								count, err := s.SourceDeliverFromLocal(key, 2, msg2)
								So(err, ShouldBeNil)
								So(count, ShouldEqual, 1)

								Convey("create batch2", func() {
									batch2, errs := s.CreateMessageBatch(t1)
									So(errs, ShouldBeNil)
									So(batch2, ShouldNotBeNil)

									Convey("mark batch2 dispatched", func() {
										report := s.MarkDispatched(batch2)
										Printf("report %v", report)
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

							err := s.SourceHandleSinkAck(t1, t1, to, ack2)
							So(err, ShouldBeNil)

							Convey("should not have updated Subscriptions", func() {
								sub, exists := s.GetSourceLocalSubscriptions(key)
								So(exists, ShouldBeTrue)
								So(sub.DeliverPolicy, ShouldEqual, jetstream.DeliverByStartSequencePolicy)
								So(sub.OptStartSeq, ShouldEqual, 2)
							})
						})

						Convey("deliver local msg2, before ack1 is received", func() {
							count, err := s.SourceDeliverFromLocal(key, 2, msg2)
							So(err, ShouldBeNil)
							So(count, ShouldEqual, 1)

							t2 := time.Now()
							batch2, errs := s.CreateMessageBatch(t2)
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

									err := s.SourceHandleSinkAck(t1, t1, to, ack1)
									So(err, ShouldBeNil)

									Convey("should have updated Subscriptions, to be deliver from sequence 4", func() {
										sub, exists := s.GetSourceLocalSubscriptions(key)
										So(exists, ShouldBeTrue)
										So(sub.DeliverPolicy, ShouldEqual, jetstream.DeliverByStartSequencePolicy)
										So(sub.OptStartSeq, ShouldEqual, 4)
									})

									Convey("HandleTargetAck ack2", func() {
										ack2 := &v1.Acknowledge{
											SetId:            batch2.ListOfMessages[0].SetId,
											SourceStreamName: "stream1",
											SequenceFrom:     2,
											SequenceTo:       4}

										err := s.SourceHandleSinkAck(t1, t1, to, ack2)
										So(err, ShouldBeNil)
									})
								})

								Convey("HandleTargetAck ack2", func() {
									ack2 := &v1.Acknowledge{
										SetId:            batch2.ListOfMessages[0].SetId,
										SourceStreamName: "stream1",
										SequenceFrom:     2,
										SequenceTo:       4}

									err := s.SourceHandleSinkAck(t1, t1, to, ack2)
									So(err, ShouldBeNil)

									Convey("should have _not_ have updated Subscriptions", func() {
										sub, exists := s.GetSourceLocalSubscriptions(key)
										So(exists, ShouldBeTrue)
										So(sub.DeliverPolicy, ShouldEqual, jetstream.DeliverByStartSequencePolicy)
										So(sub.OptStartSeq, ShouldEqual, 4)
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

func TestStateSourceBackpressure(t *testing.T) {
	now := time.Now()

	Convey("init state", t, func() {
		from := gateway.Deployment("xx")
		to := gateway.Deployment("yy")

		maxPending := testDefaultComm.MaxPendingAcksPrSubscription
		s, err := newState(from, to, testDefaultComm, nil)
		So(err, ShouldBeNil)

		Convey("assumed max pending is 10", func() {
			So(maxPending, ShouldEqual, 10)
		})

		Convey("register subscription, with this deployment as the source", func() {
			req := &v1.StartSyncRequest{
				SourceDeployment: from.String(),
				SinkDeployment:   to.String(),
				SourceStreamName: "stream1",
				SinkStreamName:   "stream2",
				ConsumerConfig:   &v1.ConsumerConfig{DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL}}
			err := s.RegisterStartSync(req)
			So(err, ShouldBeNil)

			key := SourceSubscriptionKey{SourceStreamName: "stream1"}

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
					So(s.PendingStats(now), ShouldResemble, []int{maxPending, 0})
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
						So(s.PendingStats(now), ShouldResemble, []int{maxPending, 0})
					})
				})

				Convey("deliver message, restarting subscription", func() {
					msg := &v1.Msg{Sequence: 2}
					count, err := s.SourceDeliverFromLocal(key, 0, msg)
					So(err, ShouldBeNil)
					So(count, ShouldEqual, 1)

					Convey("should have 1 outgoing messages", func() {
						So(s.PendingStats(now), ShouldResemble, []int{1, 0})
					})
				})

				Convey("create batch and dispatch", func() {
					b, err := s.CreateMessageBatch(time.Now())
					So(err, ShouldBeNil)

					report := s.MarkDispatched(b)
					So(report.IsEmpty(), ShouldBeTrue)

					Convey("should have 0 outgoing messages", func() {
						So(s.PendingStats(now), ShouldResemble, []int{0, 0})
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
					So(s.PendingStats(now), ShouldResemble, []int{8, 0})
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
						So(s.PendingStats(now), ShouldResemble, []int{maxPending, 0})
					})
				})
			})
		})
	})
}

func TestStateSourceNAK(t *testing.T) {
	Convey("init state", t, func() {
		from := gateway.Deployment("xx")
		to := gateway.Deployment("yy")

		s, err := newState(from, to, testDefaultComm, nil)
		So(err, ShouldBeNil)

		Convey("register subscription, with this deployment as the source", func() {
			req := &v1.StartSyncRequest{
				SourceDeployment: from.String(),
				SinkDeployment:   to.String(),
				SourceStreamName: "stream1",
				SinkStreamName:   "stream2",
				ConsumerConfig: &v1.ConsumerConfig{
					DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL}}
			err := s.RegisterStartSync(req)
			So(err, ShouldBeNil)

			key := SourceSubscriptionKey{SourceStreamName: "stream1"}

			Convey("deliver messages for stream1, starting from 0", func() {
				msgs := []*v1.Msg{{
					Subject:          "x.y.z",
					Data:             []byte("123"),
					Sequence:         11,
					PublishTimestamp: timestamppb.Now()}}

				_, err := s.SourceDeliverFromLocal(key, 0, msgs...)
				So(err, ShouldBeNil)

				Convey("create batch and send", func() {
					b, err := s.CreateMessageBatch(time.Now())
					So(err, ShouldBeNil)

					report := s.MarkDispatched(b)
					So(report.IsEmpty(), ShouldBeTrue)

					Convey("receive nak, with sequence_from 0", func() {
						setID := b.ListOfMessages[0].SetId
						nak := &v1.Acknowledge{
							SetId:            setID,
							IsNak:            true,
							SourceStreamName: "stream1",
							SequenceFrom:     0,
							SequenceTo:       0}

						err := s.SourceHandleSinkAck(time.Now(), time.Now(), to, nak)
						So(err, ShouldBeNil)

						Convey("should have updated Subscriptions, to be deliver from sequence 0", func() {
							sub, exists := s.GetSourceLocalSubscriptions(key)
							So(exists, ShouldBeTrue)
							So(sub.DeliverPolicy, ShouldEqual, jetstream.DeliverAllPolicy)
						})
					})

					Convey("receive nak, with sequence_from 15", func() {
						setID := b.ListOfMessages[0].SetId
						nak := &v1.Acknowledge{
							SetId:            setID,
							IsNak:            true,
							SourceStreamName: "stream1",
							SequenceFrom:     15,
							SequenceTo:       0}

						err := s.SourceHandleSinkAck(time.Now(), time.Now(), to, nak)
						So(err, ShouldBeNil)

						Convey("should have updated Subscriptions, to be deliver from sequence 15", func() {
							sub, exists := s.GetSourceLocalSubscriptions(key)
							So(exists, ShouldBeTrue)
							So(sub.DeliverPolicy, ShouldEqual, jetstream.DeliverByStartSequencePolicy)
							So(sub.OptStartSeq, ShouldEqual, 15)
						})

						Convey("deliver messages for stream1, starting from 15", func() {
							msg2 := &v1.Msg{
								Subject:          "x.y.z",
								Data:             []byte("123"),
								Sequence:         16,
								PublishTimestamp: timestamppb.Now()}

							_, err := s.SourceDeliverFromLocal(key, 15, msg2)
							So(err, ShouldBeNil)

							Convey("create batch and send", func() {
								b2, err := s.CreateMessageBatch(time.Now())
								So(err, ShouldBeNil)

								report := s.MarkDispatched(b2)
								So(report.IsEmpty(), ShouldBeTrue)

								Convey("receive ack, with sequence_from 15", func() {
									setID2 := b2.ListOfMessages[0].SetId
									ack := &v1.Acknowledge{
										SetId:            setID2,
										SourceStreamName: "stream1",
										SequenceFrom:     15,
										SequenceTo:       16}

									err := s.SourceHandleSinkAck(time.Now(), time.Now(), to, ack)
									So(err, ShouldBeNil)

									Convey("should have updated Subscriptions, to be deliver from sequence 16", func() {
										sub, exists := s.GetSourceLocalSubscriptions(key)
										So(exists, ShouldBeTrue)
										So(sub.DeliverPolicy, ShouldEqual, jetstream.DeliverByStartSequencePolicy)
										So(sub.OptStartSeq, ShouldEqual, 16)
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

func TestStateSourceInit(t *testing.T) {
	now := time.Now()

	Convey("init state, starting seq 15", t, func() {
		from := gateway.Deployment("xx")
		to := gateway.Deployment("yy")
		key := SourceSubscriptionKey{SourceStreamName: "stream1"}
		s, err := newState(from, to, testDefaultComm, map[SourceSubscriptionKey]uint64{key: 15})
		So(err, ShouldBeNil)

		Convey("register subscription, with this deployment as the source", func() {
			req := &v1.StartSyncRequest{
				SourceDeployment: from.String(),
				SinkDeployment:   to.String(),
				SourceStreamName: "stream1",
				SinkStreamName:   "stream2",
				ConsumerConfig: &v1.ConsumerConfig{
					DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL}}
			err := s.RegisterStartSync(req)
			So(err, ShouldBeNil)

			Convey("deliver messages for stream1, starting from 15", func() {
				msgs := []*v1.Msg{{
					Subject:          "x.y.z",
					Data:             []byte("123"),
					Sequence:         16,
					PublishTimestamp: timestamppb.Now()}}

				_, err := s.SourceDeliverFromLocal(key, 15, msgs...)
				So(err, ShouldBeNil)

				Convey("create batch and send", func() {
					b1, err := s.CreateMessageBatch(time.Now())
					So(err, ShouldBeNil)

					report := s.MarkDispatched(b1)
					So(report.IsEmpty(), ShouldBeTrue)

					setID1 := b1.ListOfMessages[0].SetId

					Convey("receive nak, with sequence_from 25", func() {
						nak := &v1.Acknowledge{
							SetId:            setID1,
							IsNak:            true,
							SourceStreamName: "stream1",
							SequenceFrom:     25}

						err := s.SourceHandleSinkAck(time.Now(), time.Now(), to, nak)
						So(err, ShouldBeNil)

						Convey("should have updated Subscriptions, to be deliver from sequence 25", func() {
							sub, exists := s.GetSourceLocalSubscriptions(key)
							So(exists, ShouldBeTrue)
							So(sub.DeliverPolicy, ShouldEqual, jetstream.DeliverByStartSequencePolicy)
							So(sub.OptStartSeq, ShouldEqual, 25)
						})
					})

					Convey("deliver another message for stream1", func() {
						msg3 := &v1.Msg{
							Subject:          "x.y.z",
							Data:             []byte("123"),
							Sequence:         17,
							PublishTimestamp: timestamppb.Now()}

						_, err := s.SourceDeliverFromLocal(key, 16, msg3)
						So(err, ShouldBeNil)

						Convey("receive nak, with sequence_from 25", func() {
							nak := &v1.Acknowledge{
								SetId:            setID1,
								IsNak:            true,
								SourceStreamName: "stream1",
								SequenceFrom:     25,
								SequenceTo:       0}

							err := s.SourceHandleSinkAck(time.Now(), time.Now(), to, nak)
							So(err, ShouldBeNil)

							Convey("deliver message from 25", func() {
								msg3 := &v1.Msg{
									Subject:          "x.y.z",
									Data:             []byte("123"),
									Sequence:         26,
									PublishTimestamp: timestamppb.Now()}

								_, err := s.SourceDeliverFromLocal(key, 25, msg3)
								So(err, ShouldBeNil)
							})
						})

						Convey("create batch#2 and send", func() {
							b2, err := s.CreateMessageBatch(time.Now())
							So(err, ShouldBeNil)
							setID2 := b2.ListOfMessages[0].SetId

							report := s.MarkDispatched(b2)
							So(report.IsEmpty(), ShouldBeTrue)

							Convey("receive nak from batch#1, with sequence_from 25", func() {
								nak1 := &v1.Acknowledge{
									SetId:            setID1,
									IsNak:            true,
									SourceStreamName: "stream1",
									SequenceFrom:     25,
									SequenceTo:       0}

								err := s.SourceHandleSinkAck(time.Now(), time.Now(), to, nak1)
								So(err, ShouldBeNil)

								Convey("deliver message from 25", func() {
									msg3 := &v1.Msg{
										Subject:          "x.y.z",
										Data:             []byte("123"),
										Sequence:         26,
										PublishTimestamp: timestamppb.Now()}

									_, err := s.SourceDeliverFromLocal(key, 25, msg3)
									So(err, ShouldBeNil)

									Convey("should have 1 message to send", func() {
										So(s.PendingStats(now), ShouldResemble, []int{1, 0})
									})

									Convey("create batch#3 and send", func() {
										b3, err := s.CreateMessageBatch(time.Now())
										So(err, ShouldBeNil)
										report := s.MarkDispatched(b3)
										So(report.IsEmpty(), ShouldBeTrue)

										Convey("should have pending ack for batch#3", func() {
											So(s.sourceAckWindows[key].Pending, ShouldNotBeEmpty)
										})

										Convey("receive nak from batch#2, with sequence_from 25", func() {
											nak2 := &v1.Acknowledge{
												SetId:            setID2,
												IsNak:            true,
												SourceStreamName: "stream1",
												SequenceFrom:     25,
												SequenceTo:       0}

											err := s.SourceHandleSinkAck(time.Now(), time.Now(), to, nak2)
											So(err, ShouldBeNil)

											Convey("should not reset subscription", func() {
												sub, exists := s.GetSourceLocalSubscriptions(key)
												So(exists, ShouldBeTrue)
												So(sub.DeliverPolicy, ShouldEqual, jetstream.DeliverByStartSequencePolicy)
												So(sub.OptStartSeq, ShouldEqual, 26)
											})

											Convey("deliver message from 26", func() {
												msg3 := &v1.Msg{
													Subject:          "x.y.z",
													Data:             []byte("123"),
													Sequence:         27,
													PublishTimestamp: timestamppb.Now()}

												_, err := s.SourceDeliverFromLocal(key, 26, msg3)
												So(err, ShouldBeNil)

												Convey("should have 1 message to send", func() {
													So(s.PendingStats(now), ShouldResemble, []int{1, 0})
												})
											})
										})
									})

									Convey("receive nak from batch#2, with sequence_from 25", func() {
										nak2 := &v1.Acknowledge{
											SetId:            setID2,
											IsNak:            true,
											SourceStreamName: "stream1",
											SequenceFrom:     25,
											SequenceTo:       0}

										err := s.SourceHandleSinkAck(time.Now(), time.Now(), to, nak2)
										So(err, ShouldBeNil)

										Convey("should not reset subscription", func() {
											sub, exists := s.GetSourceLocalSubscriptions(key)
											So(exists, ShouldBeTrue)
											So(sub.DeliverPolicy, ShouldEqual, jetstream.DeliverByStartSequencePolicy)
											So(sub.OptStartSeq, ShouldEqual, 26)
										})

										Convey("should have 1 message to send", func() {
											So(s.PendingStats(now), ShouldResemble, []int{1, 0})
										})
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

func TestStateSourceAckTimeout(t *testing.T) {
	Convey("init state", t, func() {
		from := gateway.Deployment("xx")
		to := gateway.Deployment("yy")

		ackTimeout := testDefaultComm.AckTimeoutPrSubscription
		s, err := newState(from, to, testDefaultComm, nil)
		So(err, ShouldBeNil)

		Convey("assumed ack timeout 1 second", func() {
			So(ackTimeout, ShouldEqual, time.Second)
		})

		Convey("register subscription, with this deployment as the source", func() {
			req := &v1.StartSyncRequest{
				SourceDeployment: from.String(),
				SinkDeployment:   to.String(),
				SourceStreamName: "stream1",
				SinkStreamName:   "stream2",
				ConsumerConfig:   &v1.ConsumerConfig{DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL}}
			err := s.RegisterStartSync(req)
			So(err, ShouldBeNil)

			key := SourceSubscriptionKey{SourceStreamName: "stream1"}

			Convey("deliver message", func() {
				msg1 := &v1.Msg{
					Subject:  "x.y.z",
					Data:     []byte("123"),
					Sequence: 1}

				_, err := s.SourceDeliverFromLocal(key, 0, msg1)
				So(err, ShouldBeNil)

				Convey("create batch#1 and send", func() {
					t0 := time.Now()
					b1, err := s.CreateMessageBatch(t0)
					So(err, ShouldBeNil)

					report := s.MarkDispatched(b1)
					So(report.IsEmpty(), ShouldBeTrue)

					Convey("no ack received within timeout (1 sec), should retransmit after 2 sec", func() {
						t1 := t0.Add(2 * time.Second)

						Convey("should have 1 message to send", func() {
							So(s.PendingStats(t1), ShouldResemble, []int{1, 0})
						})

						Convey("create batch#2", func() {
							b2, err := s.CreateMessageBatch(t1)
							So(err, ShouldBeNil)

							Convey("should have 1 Msgs to send", func() {
								So(b2.GetListOfMessages(), ShouldHaveLength, 1)
								m1 := b1.ListOfMessages[0]
								m2 := b2.ListOfMessages[0]

								Convey("containing 1 message", func() {
									So(m2.Messages, ShouldHaveLength, 1)
								})
								Convey("with same set ID", func() {
									So(m2.SetId, ShouldEqual, m1.SetId)
								})
								Convey("with same last sequence", func() {
									So(m2.LastSequence, ShouldEqual, m1.LastSequence)
								})
							})

							Convey("mark batch#2 dispatched", func() {
								report := s.MarkDispatched(b2)
								So(report.IsEmpty(), ShouldBeTrue)

								Convey("no ack received within timeout (1 sec), should retransmit after 5 sec", func() {
									t2 := t1.Add(5 * time.Second)

									Convey("should have 1 message to send", func() {
										So(s.PendingStats(t2), ShouldResemble, []int{1, 0})
									})
								})

								Convey("no ack received within timeout (1 sec), should NOT retransmit after 4 sec", func() {
									t2 := t1.Add(4 * time.Second)

									Convey("should have NO message to send", func() {
										So(s.PendingStats(t2), ShouldResemble, []int{0, 0})
									})
								})
							})
						})
					})

					Convey("no ack received within timeout (1 sec), should NOT retransmit yet (because of retryer)", func() {
						t1 := t0.Add(1 * time.Second)

						Convey("should have no messages to send", func() {
							So(s.PendingStats(t1), ShouldResemble, []int{0, 0})
						})

						Convey("create batch#2", func() {
							b2, err := s.CreateMessageBatch(t1)
							So(err, ShouldBeNil)

							Convey("should have no Msgs to send", func() {
								So(b2.GetListOfMessages(), ShouldHaveLength, 0)
							})
						})
					})
				})
			})
		})
	})
}

func TestStateSourceHeartbeat(t *testing.T) {
	Convey("init state", t, func() {
		from := gateway.Deployment("xx")
		to := gateway.Deployment("yy")

		heartbeatInterval := testDefaultComm.HeartbeatIntervalPrSubscription
		s, err := newState(from, to, testDefaultComm, nil)
		So(err, ShouldBeNil)

		Convey("assumed heartbeat interval of 1 min", func() {
			So(heartbeatInterval, ShouldEqual, time.Minute)
		})

		Convey("register subscription, with this deployment as the source", func() {
			req := &v1.StartSyncRequest{
				SourceDeployment: from.String(),
				SinkDeployment:   to.String(),
				SourceStreamName: "stream1",
				SinkStreamName:   "stream2",
				ConsumerConfig:   &v1.ConsumerConfig{DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL}}
			err := s.RegisterStartSync(req)
			So(err, ShouldBeNil)

			key := SourceSubscriptionKey{SourceStreamName: "stream1"}

			Convey("deliver message", func() {
				msg1 := &v1.Msg{
					Subject:  "x.y.z",
					Data:     []byte("123"),
					Sequence: 1}

				_, err := s.SourceDeliverFromLocal(key, 0, msg1)
				So(err, ShouldBeNil)

				Convey("create batch#1 and send", func() {
					t1 := time.Now()
					b1, err := s.CreateMessageBatch(t1)
					So(err, ShouldBeNil)

					report := s.MarkDispatched(b1)
					So(report.IsEmpty(), ShouldBeTrue)

					So(s.sourceOutgoing[key], ShouldHaveLength, 0)

					Convey("receive ack for batch#1", func() {
						ack1 := &v1.Acknowledge{
							SetId:            b1.ListOfMessages[0].SetId,
							SourceStreamName: "stream1",
							SequenceFrom:     0,
							SequenceTo:       1}

						err := s.SourceHandleSinkAck(t1, t1, to, ack1)
						So(err, ShouldBeNil)

						Convey("no ack received within heartbeat interval (1 min), should send heartbeat after 1 min", func() {
							t2 := t1.Add(1 * time.Minute)

							Convey("ack window", func() {
								w := s.sourceAckWindows[key]
								Convey("last activity should equal t1", func() {
									So(w.LastActivity, ShouldEqual, t1)
								})
								Convey("should have nothing pending", func() {
									So(w.Pending, ShouldBeEmpty)
								})
								Convey("should sent heartbeat", func() {
									So(w.ShouldSentHeartbeat(t2, heartbeatInterval), ShouldBeTrue)
								})
							})

							Convey("should have 1 message to send", func() {
								So(s.PendingStats(t2), ShouldResemble, []int{1, 0})
							})

							Convey("create batch#2", func() {
								b2, err := s.CreateMessageBatch(t2)
								So(err, ShouldBeNil)

								Convey("should have 1 Msgs to send", func() {
									So(b2.GetListOfMessages(), ShouldHaveLength, 1)
								})
							})
						})

						Convey("no ack received just before heartbeat interval (1 min)", func() {
							t2 := t1.Add(59 * time.Second)

							Convey("should have no messages to send", func() {
								So(s.PendingStats(t2), ShouldResemble, []int{0, 0})
							})

							Convey("create batch#2", func() {
								b2, err := s.CreateMessageBatch(t2)
								So(err, ShouldBeNil)

								Convey("should have 0 Msgs to send", func() {
									So(b2.GetListOfMessages(), ShouldHaveLength, 0)
								})
							})
						})
					})

					Convey("no ack received within heartbeat interval (1 min), but pending ack", func() {
						t2 := t1.Add(1 * time.Minute)

						Convey("should have retransmit (but not heartbeat) to send", func() {
							So(s.PendingStats(t2), ShouldResemble, []int{1, 0})

							Convey("create batch#2", func() {
								b2, err := s.CreateMessageBatch(t2)
								So(err, ShouldBeNil)

								Convey("should have 1 Msgs to send, matching retransmit", func() {
									So(b2.GetListOfMessages(), ShouldHaveLength, 1)
									m2 := b2.ListOfMessages[0]
									So(m2.GetSetId(), ShouldEqual, b1.ListOfMessages[0].GetSetId())
								})
							})
						})
					})
				})
			})
		})
	})
}

func TestStateSourceMaxPayloadSize(t *testing.T) {
	Convey("init state", t, func() {
		from := gateway.Deployment("xx")
		to := gateway.Deployment("yy")

		maxSize := testDefaultComm.MaxAccumulatedPayloadSizeBytes
		s, err := newState(from, to, testDefaultComm, nil)
		So(err, ShouldBeNil)

		Convey("assumed max size is 1kB", func() {
			So(maxSize, ShouldEqual, 1024)
		})

		Convey("register subscription, with this deployment as the source", func() {
			req := &v1.StartSyncRequest{
				SourceDeployment: from.String(),
				SinkDeployment:   to.String(),
				SourceStreamName: "stream1",
				SinkStreamName:   "stream2",
				ConsumerConfig: &v1.ConsumerConfig{
					DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL}}
			err := s.RegisterStartSync(req)
			So(err, ShouldBeNil)

			key := SourceSubscriptionKey{SourceStreamName: "stream1"}

			Convey("deliver msg to local, smaller than max size", func() {
				msg1 := &v1.Msg{
					Subject:  "x.y.z",
					Data:     []byte("123"),
					Sequence: 1}
				count, err := s.SourceDeliverFromLocal(key, 0, msg1)
				So(err, ShouldBeNil)
				So(count, ShouldEqual, 1)

				Convey("create batch, should have 1 message", func() {
					b, err := s.CreateMessageBatch(time.Now())
					So(err, ShouldBeNil)
					So(b.GetListOfMessages(), ShouldHaveLength, 1)
					So(b.GetListOfMessages()[0].GetMessages(), ShouldHaveLength, 1)
				})

				Convey("then deliver msg, just below max size", func() {
					msg1 := &v1.Msg{
						Subject:  "x.y.z",
						Data:     make([]byte, maxSize-1),
						Sequence: 2}

					_, err := s.SourceDeliverFromLocal(key, 1, msg1)
					So(err, ShouldBeNil)
					So(count, ShouldEqual, 1)

					Convey("should have 2 outgoing messages (in pending stats)", func() {
						So(s.PendingStats(time.Now()), ShouldResemble, []int{2, 0})
					})

					Convey("create batch, should only include 1 message, not the 2nd message", func() {
						b, err := s.CreateMessageBatch(time.Now())
						So(err, ShouldBeNil)
						So(b.GetListOfMessages(), ShouldHaveLength, 1)
						So(b.GetListOfMessages()[0].GetMessages(), ShouldHaveLength, 1)

						Convey("dispatch batch", func() {
							report := s.MarkDispatched(b)
							So(report.IsEmpty(), ShouldBeTrue)

							Convey("should have 1 outgoing messages", func() {
								So(s.PendingStats(time.Now()), ShouldResemble, []int{1, 0})
							})
						})
					})
				})
			})

			Convey("deliver msg to local, bigger than max size", func() {
				msg1 := &v1.Msg{
					Subject:  "x.y.z",
					Data:     make([]byte, maxSize+1),
					Sequence: 1}
				_, err := s.SourceDeliverFromLocal(key, 0, msg1)

				Convey("should complain with ErrPayloadTooLarge", func() {
					So(err, ShouldNotBeNil)
					So(errors.Is(err, ErrPayloadTooLarge), ShouldBeTrue)
				})
			})
		})
	})
}
