package nats_sync

import (
	"testing"
	"time"

	"github.com/bredtape/gateway"
	v1 "github.com/bredtape/gateway/nats_sync/v1"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/smartystreets/goconvey/convey"
)

func TestState(t *testing.T) {
	Convey("init state", t, func() {
		d := gateway.Deployment("xx")
		s := newState(d, map[gateway.Deployment]CommunicationSettings{})

		Convey("register subscription, with this deployment as the source", func() {
			req := &v1.SubscribeRequest{
				SourceDeployment: "xx",
				ReplyDeployment:  "xx",
				TargetDeployment: "yy",
				SourceStreamName: "stream1",
				TargetStreamName: "stream2",
				ConsumerConfig:   &v1.ConsumerConfig{DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL}}
			err := s.RegisterSubscription(req)
			So(err, ShouldBeNil)

			Convey("should have source-subscription to deliver last", func() {
				key := SubscriptionKey{
					TargetDeployment: "yy",
					SourceStreamName: "stream1"}
				So(s.LocalSubscriptions, ShouldContainKey, key)
				So(s.LocalSubscriptions[key], ShouldResemble, sourceSubscription{
					SubscriptionKey: key,
					DeliverPolicy:   jetstream.DeliverLastPolicy})

				Convey("deliver last msg for stream1", func() {
					batch1 := &v1.MsgBatch{
						SourceStreamName: "stream1",
						TargetDeployment: "yy",
						ConsumerConfig: &v1.ConsumerConfig{
							DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL},
						Messages: []*v1.Msg{
							{
								Subject:          "x.y.z",
								Data:             []byte("123"),
								SourceSequence:   2,
								PublishTimestamp: 5}}}
					err := s.SourceDeliverFromLocal(batch1)
					So(err, ShouldBeNil)

					Convey("should have outgoing batch for stream1", func() {
						So(s.Outgoing, ShouldContainKey, key)
						So(s.Outgoing[key], ShouldNotBeEmpty)
						batch := s.Outgoing[key]

						Convey("with source stream", func() {
							So(batch.SourceStreamName, ShouldEqual, "stream1")
						})
						Convey("with target deployment", func() {
							So(batch.TargetDeployment, ShouldEqual, "yy")
						})
						Convey("with consumer config, deliver all", func() {
							So(batch.ConsumerConfig.DeliverPolicy, ShouldEqual, v1.DeliverPolicy_DELIVER_POLICY_ALL)
						})
					})

					Convey("mark dispatched", func() {
						t1 := time.Now()
						err := s.SourceMarkDispatched(t1, batch1)
						So(err, ShouldBeNil)

						Convey("should no longer have any Outgoing messages", func() {
							So(s.Outgoing, ShouldNotContainKey, key)
						})

						Convey("HandleIncoming with ack for stream1, also at t1", func() {
							me1 := &v1.MessageExchange{
								ToDeployment:   "xx",
								FromDeployment: "yy",
								SentTimestamp:  toUnixTime(t1),
								Acknowledges: []*v1.Acknowledge{{
									SourceStreamName:   "stream1",
									SourceSequenceFrom: 0,
									SourceSequenceTo:   2}}}

							err := s.HandleIncoming(t1, me1)
							So(err, ShouldBeNil)

							Convey("should have updated Subscriptions, to be deliver from sequene 2", func() {
								So(s.LocalSubscriptions[key], ShouldResemble,
									sourceSubscription{
										SubscriptionKey: key,
										DeliverPolicy:   jetstream.DeliverByStartSequencePolicy,
										OptStartSeq:     2})
							})
						})
					})
				})
			})
		})
	})
}
