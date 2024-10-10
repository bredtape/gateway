package nats_sync

import (
	"testing"
	"time"

	"github.com/bredtape/gateway"
	v1 "github.com/bredtape/gateway/nats_sync/v1"
	"github.com/nats-io/nats.go/jetstream"
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
			SourceSequence:   2,
			PublishTimestamp: 5}
		msg2 := &v1.Msg{
			Subject:          "x.y.z",
			Data:             []byte("123"),
			SourceSequence:   4,
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

			key := SubscriptionKey{
				TargetDeployment: target,
				SourceStreamName: "stream1"}

			Convey("should have source-subscription to deliver last", func() {
				So(s.SourceLocalSubscriptions, ShouldContainKey, key)
				So(s.SourceLocalSubscriptions[key], ShouldResemble, SourceSubscription{
					SubscriptionKey: key,
					DeliverPolicy:   jetstream.DeliverLastPolicy})
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
						err := s.SourceMarkDispatched(batch1)
						So(err, ShouldBeNil)

						Convey("create another batch", func() {
							batch2, errs := s.CreateMessageBatch(t1, target)
							So(errs, ShouldBeNil)

							Convey("should be empty", func() {
								So(batch2, ShouldBeNil)
							})
						})

						Convey("HandleTargetAck with ack for stream1, also at t1", func() {
							ack1 := &v1.Acknowledge{
								SetId:              batch1.ListOfMessages[0].SetId,
								SourceStreamName:   "stream1",
								SourceSequenceFrom: 0,
								SourceSequenceTo:   2}

							err := s.SourceHandleTargetAck(t1, t1, target, ack1)
							So(err, ShouldBeNil)

							Convey("should have updated Subscriptions, to be deliver from sequence 2", func() {
								So(s.SourceLocalSubscriptions[key], ShouldResemble,
									SourceSubscription{
										SubscriptionKey: key,
										DeliverPolicy:   jetstream.DeliverByStartSequencePolicy,
										OptStartSeq:     2})
							})

							Convey("deliver msg2, after ack1 is received", func() {
								err := s.SourceDeliverFromLocal(key, 2, msg2)
								So(err, ShouldBeNil)

								Convey("create batch2", func() {
									batch2, errs := s.CreateMessageBatch(t1, target)
									So(errs, ShouldBeNil)
									So(batch2, ShouldNotBeNil)

									Convey("mark batch2 dispatched", func() {
										err := s.SourceMarkDispatched(batch2)
										So(err, ShouldBeNil)
									})
								})
							})
						})

						Convey("HandleTargetAck, with SetID _not_ matching", func() {
							ack2 := &v1.Acknowledge{
								SetId:              NewSetID().GetBytes(),
								SourceStreamName:   "stream1",
								SourceSequenceFrom: 0,
								SourceSequenceTo:   2}

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
								err := s.SourceMarkDispatched(batch2)
								So(err, ShouldBeNil)

								Convey("HandleTargetAck ack1", func() {
									ack1 := &v1.Acknowledge{
										SetId:              batch1.ListOfMessages[0].SetId,
										SourceStreamName:   "stream1",
										SourceSequenceFrom: 0,
										SourceSequenceTo:   2}

									err := s.SourceHandleTargetAck(t1, t1, target, ack1)
									So(err, ShouldBeNil)

									Convey("should have updated Subscriptions, to be deliver from sequence 2", func() {
										So(s.SourceLocalSubscriptions[key], ShouldResemble,
											SourceSubscription{
												SubscriptionKey: key,
												DeliverPolicy:   jetstream.DeliverByStartSequencePolicy,
												OptStartSeq:     2})
									})

									Convey("HandleTargetAck ack2", func() {
										ack2 := &v1.Acknowledge{
											SetId:              batch2.ListOfMessages[0].SetId,
											SourceStreamName:   "stream1",
											SourceSequenceFrom: 2,
											SourceSequenceTo:   4}

										err := s.SourceHandleTargetAck(t1, t1, target, ack2)
										So(err, ShouldBeNil)

										Convey("should have updated Subscriptions, to be deliver from sequence 4", func() {
											So(s.SourceLocalSubscriptions[key], ShouldResemble,
												SourceSubscription{
													SubscriptionKey: key,
													DeliverPolicy:   jetstream.DeliverByStartSequencePolicy,
													OptStartSeq:     4})
										})
									})
								})

								Convey("HandleTargetAck ack2", func() {
									ack2 := &v1.Acknowledge{
										SetId:              batch2.ListOfMessages[0].SetId,
										SourceStreamName:   "stream1",
										SourceSequenceFrom: 2,
										SourceSequenceTo:   4}

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
			SourceSequence:   2,
			PublishTimestamp: 5}
		msg2 := &v1.Msg{
			Subject:          "x.y.z",
			Data:             []byte("123"),
			SourceSequence:   4,
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
				key := SubscriptionKey{
					TargetDeployment: target,
					SourceStreamName: "stream1"}
				So(s.SourceLocalSubscriptions, ShouldNotContainKey, key)
			})

			Convey("deliver last msg for stream1", func() {
				err := s.TargetDeliverFromLocal(key, 0, msg1)
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

					})
				})
			})
		})
	})
}
