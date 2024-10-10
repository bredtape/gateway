package nats_sync

import (
	"testing"
	"time"

	"github.com/bredtape/gateway"
	v1 "github.com/bredtape/gateway/nats_sync/v1"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
)

func TestStateSource(t *testing.T) {
	Convey("init state", t, func() {
		source := gateway.Deployment("xx")
		target := gateway.Deployment("yy")
		s := newState(source, map[gateway.Deployment]CommunicationSettings{})

		msg1 := &v1.Msg{
			Subject:          "x.y.z",
			Data:             []byte("123"),
			Sequence:         2,
			PublishTimestamp: 5}
		msg2 := &v1.Msg{
			Subject:          "x.y.z",
			Data:             []byte("123"),
			Sequence:         4,
			PublishTimestamp: 6}

		Convey("register subscription, with this deployment as the source", func() {
			req := &v1.SubscribeRequest{
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
				err := s.SourceDeliverFromLocal(key, 0, msg1)
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
								err := s.SourceDeliverFromLocal(key, 2, msg2)
								So(err, ShouldBeNil)

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
								SetId:            NewSetID().GetBytes(),
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
							err := s.SourceDeliverFromLocal(key, 2, msg2)
							So(err, ShouldBeNil)

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

// test state for where messages are incoming to use
func TestStateTarget(t *testing.T) {
	Convey("init state", t, func() {
		source := gateway.Deployment("xx")
		target := gateway.Deployment("yy")
		s := newState(target, map[gateway.Deployment]CommunicationSettings{})

		msg1 := &v1.Msg{
			Subject:          "x.y.z",
			Data:             []byte("123"),
			Sequence:         2,
			PublishTimestamp: 5}
		msg2 := &v1.Msg{
			Subject:          "x.y.z",
			Data:             []byte("123"),
			Sequence:         4,
			PublishTimestamp: 6}

		Convey("register subscription, with this deployment as the target", func() {
			req := &v1.SubscribeRequest{
				SourceDeployment: source.String(),
				ReplyDeployment:  source.String(),
				TargetDeployment: target.String(),
				SourceStreamName: "stream1",
				TargetStreamName: "stream2",
				ConsumerConfig:   &v1.ConsumerConfig{DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL}}
			err := s.RegisterSubscription(req)
			So(err, ShouldBeNil)

			Convey("should not have local subscription", func() {
				key := SourceSubscriptionKey{
					TargetDeployment: target,
					SourceStreamName: "stream1"}
				So(s.SourceLocalSubscriptions, ShouldNotContainKey, key)
			})

			key := TargetSubscriptionKey{
				SourceDeployment: source,
				SourceStreamName: "stream1"}

			Convey("should have target subscription", func() {
				So(s.targetSubscription, ShouldContainKey, key)
			})

			Convey("deliver first msg for stream1", func() {
				msgs1 := &v1.Msgs{
					SetId:            NewSetID().GetBytes(),
					SourceDeployment: "xx",
					TargetDeployment: "yy",
					SourceStreamName: "stream1",
					ConsumerConfig:   &v1.ConsumerConfig{DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL},
					LastSequence:     0,
					Messages:         []*v1.Msg{msg1}}

				t0 := time.Now()
				err := s.TargetDeliverFromRemote(t0, msgs1)
				So(err, ShouldBeNil)

				Convey("commit set ID", func() {
					err := s.TargetCommit(msgs1)
					So(err, ShouldBeNil)

					Convey("should not have incoming messages", func() {
						So(s.TargetIncoming[key], ShouldBeEmpty)
					})

					Convey("create batch", func() {
						b, err := s.CreateMessageBatch(time.Now(), source)
						So(err, ShouldBeNil)
						So(b, ShouldNotBeNil)

						Convey("batch should have ack for msg1", func() {
							So(b.Acknowledges, ShouldHaveLength, 1)
							ack := b.Acknowledges[0]

							Convey("matching set ID", func() {
								So(ack.SetId, ShouldResemble, msgs1.SetId)
							})
							Convey("matching source stream", func() {
								So(ack.SourceStreamName, ShouldEqual, "stream1")
							})
							Convey("matching sequence range", func() {
								So(ack.SequenceFrom, ShouldEqual, 0)
								So(ack.SequenceTo, ShouldEqual, 2)
							})
						})

						Convey("mark as dispatched", func() {
							report := s.MarkDispatched(b)
							Printf("report %v", report)
							So(report.IsEmpty(), ShouldBeTrue)

							Convey("create another batch, should have nothing to send", func() {
								b, err := s.CreateMessageBatch(time.Now(), source)
								So(err, ShouldBeNil)
								So(b, ShouldBeNil)
							})
						})
					})
				})

				Convey("deliver 2nd msg for stream (before commit of the 1st)", func() {
					msgs2 := &v1.Msgs{
						SetId:            NewSetID().GetBytes(),
						SourceDeployment: "xx",
						TargetDeployment: "yy",
						SourceStreamName: "stream1",
						ConsumerConfig:   &v1.ConsumerConfig{DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL},
						LastSequence:     2,
						Messages:         []*v1.Msg{msg2}}

					t1 := time.Now()
					err := s.TargetDeliverFromRemote(t1, msgs2)
					So(err, ShouldBeNil)

					Convey("commit set ID", func() {
						err := s.TargetCommit(msgs2)

						Convey("should have ErrSourceSequenceBroken", func() {
							So(err, ShouldNotBeNil)
							So(errors.Is(err, ErrSourceSequenceBroken), ShouldBeTrue)
						})
					})
				})
			})
		})
	})
}
