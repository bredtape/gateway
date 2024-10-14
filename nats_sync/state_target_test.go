package nats_sync

import (
	"testing"
	"time"

	"github.com/bredtape/gateway"
	v1 "github.com/bredtape/gateway/nats_sync/v1"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// test state for where messages are incoming to use
func TestStateTargetBasic(t *testing.T) {
	Convey("init state", t, func() {
		source := gateway.Deployment("xx")
		target := gateway.Deployment("yy")
		s, err := newState(target, map[gateway.Deployment]CommunicationSettings{
			source: testDefaultComm})
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

		Convey("register subscription, with this deployment as the target", func() {
			req := &v1.StartSyncRequest{
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
					SetId:            NewSetID().String(),
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
						SetId:            NewSetID().String(),
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

func TestStateTargetBackoff(t *testing.T) {
	Convey("init state", t, func() {
		source := gateway.Deployment("xx")
		target := gateway.Deployment("yy")
		s, err := newState(target, map[gateway.Deployment]CommunicationSettings{
			source: testDefaultComm})
		So(err, ShouldBeNil)

		maxPending := testDefaultComm.MaxPendingIncomingMessagesPrSubscription
		Convey("check that MaxPendingIncomingMessagesPrSubscription is 10", func() {
			So(maxPending, ShouldEqual, 10)
		})

		Convey("register subscription, with this deployment as the target", func() {
			req := &v1.StartSyncRequest{
				SourceDeployment: source.String(),
				ReplyDeployment:  source.String(),
				TargetDeployment: target.String(),
				SourceStreamName: "stream1",
				TargetStreamName: "stream2",
				ConsumerConfig:   &v1.ConsumerConfig{DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL}}
			err := s.RegisterSubscription(req)
			So(err, ShouldBeNil)

			key := TargetSubscriptionKey{
				SourceDeployment: source,
				SourceStreamName: "stream1"}

			Convey("deliver 10 msg", func() {
				xs := make([]*v1.Msg, 0)
				for i := 0; i < maxPending; i++ {
					xs = append(xs, &v1.Msg{
						Subject:          "x.y.z",
						Data:             []byte("123"),
						Sequence:         uint64(i + 1),
						PublishTimestamp: timestamppb.Now()})
				}

				msgs1 := &v1.Msgs{
					SetId:            NewSetID().String(),
					SourceDeployment: "xx",
					TargetDeployment: "yy",
					SourceStreamName: "stream1",
					ConsumerConfig:   &v1.ConsumerConfig{DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL},
					LastSequence:     0,
					Messages:         xs}

				t0 := time.Now()
				err := s.TargetDeliverFromRemote(t0, msgs1)
				So(err, ShouldBeNil)

				Convey("should have 1 incoming Msgs", func() {
					So(s.TargetIncoming[key], ShouldHaveLength, 1)
				})

				Convey("commit set ID", func() {
					err := s.TargetCommit(msgs1)
					So(err, ShouldBeNil)

					Convey("should not have incoming messages", func() {
						So(s.TargetIncoming[key], ShouldBeEmpty)
					})
				})

				Convey("deliver 1 more msg", func() {
					msgs2 := &v1.Msgs{
						SetId:            NewSetID().String(),
						SourceDeployment: "xx",
						TargetDeployment: "yy",
						SourceStreamName: "stream1",
						ConsumerConfig:   &v1.ConsumerConfig{DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL},
						LastSequence:     10,
						Messages: []*v1.Msg{{
							Subject:          "x.y.z",
							Data:             []byte("123"),
							Sequence:         11,
							PublishTimestamp: timestamppb.Now()}}}

					t1 := time.Now()
					err := s.TargetDeliverFromRemote(t1, msgs2)
					Convey("should have ErrBackoff", func() {
						So(err, ShouldNotBeNil)
						So(errors.Is(err, ErrBackoff), ShouldBeTrue)
					})

					Convey("should still have 1 incoming Msgs", func() {
						So(s.TargetIncoming[key], ShouldHaveLength, 1)
					})
				})
			})
		})
	})
}
