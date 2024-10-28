package sync

import (
	"testing"
	"time"

	"github.com/bredtape/gateway"
	v1 "github.com/bredtape/gateway/sync/v1"
	"github.com/pkg/errors"
	. "github.com/smartystreets/goconvey/convey"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// test state for where messages are incoming
func TestStateSinkBasic(t *testing.T) {
	Convey("init state", t, func() {
		fromSink := gateway.Deployment("yy") // sink in this scenario
		toSource := gateway.Deployment("xx") // source in this scenario
		s, err := newState(fromSink, toSource, testDefaultComm, nil)
		So(err, ShouldBeNil)

		msg1 := &v1.Msg{
			Subject:            "x.y.z",
			Data:               []byte("123"),
			Sequence:           2,
			PublishedTimestamp: timestamppb.Now()}
		msg2 := &v1.Msg{
			Subject:            "x.y.z",
			Data:               []byte("123"),
			Sequence:           4,
			PublishedTimestamp: timestamppb.Now()}

		Convey("register subscription, with this deployment as the sink", func() {
			req := &v1.StartSyncRequest{
				SourceDeployment: "xx",
				SinkDeployment:   "yy",
				SourceStreamName: "stream1",
				ConsumerConfig:   &v1.ConsumerConfig{DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL}}
			err := s.RegisterStartSync(req)
			So(err, ShouldBeNil)

			Convey("should not have local subscription", func() {
				key := SourceSubscriptionKey{SourceStreamName: "stream1"}
				_, exists := s.GetSourceLocalSubscription(key)
				So(exists, ShouldBeFalse)
			})

			key := SinkSubscriptionKey{SourceStreamName: "stream1"}

			Convey("should have sink subscription", func() {
				So(s.sinkSubscription, ShouldContainKey, key)
			})

			Convey("deliver first msg for stream1", func() {
				msgs1 := &v1.Msgs{
					SetId:            NewSetID().String(),
					SourceDeployment: "xx",
					SinkDeployment:   "yy",
					SourceStreamName: "stream1",
					ConsumerConfig:   &v1.ConsumerConfig{DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL},
					LastSequence:     0,
					Messages:         []*v1.Msg{msg1}}

				t0 := time.Now()
				err := s.SinkDeliverFromRemote(t0, msgs1)
				So(err, ShouldBeNil)

				Convey("should have incoming", func() {
					So(s.SinkIncoming[key], ShouldHaveLength, 1)
				})

				Convey("commit set ID", func() {
					err := s.SinkCommit(msgs1)
					So(err, ShouldBeNil)

					Convey("should not have incoming messages", func() {
						So(s.SinkIncoming[key], ShouldBeEmpty)
					})

					Convey("create batch", func() {
						b, err := s.CreateMessageBatch(time.Now())
						So(err, ShouldBeNil)
						So(b, ShouldNotBeNil)

						Convey("from deployment", func() {
							So(b.FromDeployment, ShouldEqual, "yy")
						})
						Convey("to deployment", func() {
							So(b.ToDeployment, ShouldEqual, "xx")
						})

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
								b, err := s.CreateMessageBatch(time.Now())
								So(err, ShouldBeNil)
								So(b, ShouldBeNil)
							})
						})
					})

					Convey("deliver 2nd msg for stream1", func() {
						msgs2 := &v1.Msgs{
							SetId:            NewSetID().String(),
							SourceDeployment: "xx",
							SinkDeployment:   "yy",
							SourceStreamName: "stream1",
							ConsumerConfig:   &v1.ConsumerConfig{DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL},
							LastSequence:     2,
							Messages:         []*v1.Msg{msg2}}

						t1 := time.Now()
						err := s.SinkDeliverFromRemote(t1, msgs2)
						So(err, ShouldBeNil)

						Convey("should have incoming", func() {
							So(s.SinkIncoming[key], ShouldHaveLength, 1)
							So(s.SinkIncoming[key][0].Messages, ShouldHaveLength, 1)
						})

						Convey("commit batch#2", func() {
							err := s.SinkCommit(msgs2)
							So(err, ShouldBeNil)

							Convey("should not have incoming messages", func() {
								So(s.SinkIncoming[key], ShouldBeEmpty)
							})
						})
					})

					Convey("deliver 2nd msg, empty", func() {
						msgs2 := &v1.Msgs{
							SetId:            NewSetID().String(),
							SourceDeployment: "xx",
							SinkDeployment:   "yy",
							SourceStreamName: "stream1",
							ConsumerConfig:   &v1.ConsumerConfig{DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL},
							LastSequence:     2,
							Messages:         nil}

						t1 := time.Now()
						err := s.SinkDeliverFromRemote(t1, msgs2)
						So(err, ShouldBeNil)

						Convey("should have incoming, but empty", func() {
							So(s.SinkIncoming[key], ShouldHaveLength, 1)
							So(s.SinkIncoming[key][0].Messages, ShouldBeEmpty)
						})
					})
				})

				Convey("deliver 2nd msg for stream (before commit of the 1st)", func() {
					msgs2 := &v1.Msgs{
						SetId:            NewSetID().String(),
						SourceDeployment: "xx",
						SinkDeployment:   "yy",
						SourceStreamName: "stream1",
						ConsumerConfig:   &v1.ConsumerConfig{DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL},
						LastSequence:     2,
						Messages:         []*v1.Msg{msg2}}

					t1 := time.Now()
					err := s.SinkDeliverFromRemote(t1, msgs2)
					So(err, ShouldBeNil)

					Convey("commit set ID", func() {
						err := s.SinkCommit(msgs2)

						Convey("should have ErrSourceSequenceBroken", func() {
							So(err, ShouldNotBeNil)
							So(errors.Is(err, ErrSourceSequenceBroken), ShouldBeTrue)
						})
					})
				})
			})

			Convey("register subscription again", func() {
				err := s.RegisterStartSync(req)
				Convey("should not have err", func() {
					So(err, ShouldBeNil)
				})
			})
		})
	})
}

func TestStateSinkBackpressure(t *testing.T) {
	from := gateway.Deployment("xx")
	to := gateway.Deployment("yy")

	Convey("init state", t, func() {
		// with 'to' as the sink
		s, err := newState(to, from, testDefaultComm, nil)
		So(err, ShouldBeNil)

		maxPending := testDefaultComm.PendingIncomingMessagesPrSubscriptionMaxBuffered
		Convey("check that 'PendingIncomingMessagesPrSubscriptionMaxBuffered' is 10", func() {
			So(maxPending, ShouldEqual, 10)
		})

		Convey("register subscription, with this deployment as the sink", func() {
			req := &v1.StartSyncRequest{
				SourceDeployment: "xx",
				SinkDeployment:   "yy",
				SourceStreamName: "stream1",
				ConsumerConfig:   &v1.ConsumerConfig{DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL}}
			err := s.RegisterStartSync(req)
			So(err, ShouldBeNil)

			key := SinkSubscriptionKey{SourceStreamName: "stream1"}

			Convey("deliver 10 msg", func() {
				xs := make([]*v1.Msg, 0)
				for i := 0; i < maxPending; i++ {
					xs = append(xs, &v1.Msg{
						Subject:            "x.y.z",
						Data:               []byte("123"),
						Sequence:           uint64(i + 1),
						PublishedTimestamp: timestamppb.Now()})
				}

				msgs1 := &v1.Msgs{
					SetId:            NewSetID().String(),
					SourceDeployment: "xx",
					SinkDeployment:   "yy",
					SourceStreamName: "stream1",
					ConsumerConfig:   &v1.ConsumerConfig{DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL},
					LastSequence:     0,
					Messages:         xs}

				t0 := time.Now()
				err := s.SinkDeliverFromRemote(t0, msgs1)
				So(err, ShouldBeNil)

				Convey("should have 1 incoming Msgs", func() {
					So(s.SinkIncoming[key], ShouldHaveLength, 1)
				})

				Convey("commit set ID", func() {
					err := s.SinkCommit(msgs1)
					So(err, ShouldBeNil)

					Convey("should not have incoming messages", func() {
						So(s.SinkIncoming[key], ShouldBeEmpty)
					})
				})

				Convey("deliver 1 more msg", func() {
					msgs2 := &v1.Msgs{
						SetId:            NewSetID().String(),
						SourceDeployment: "xx",
						SinkDeployment:   "yy",
						SourceStreamName: "stream1",
						ConsumerConfig:   &v1.ConsumerConfig{DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL},
						LastSequence:     10,
						Messages: []*v1.Msg{{
							Subject:            "x.y.z",
							Data:               []byte("123"),
							Sequence:           11,
							PublishedTimestamp: timestamppb.Now()}}}

					t1 := time.Now()
					err := s.SinkDeliverFromRemote(t1, msgs2)
					Convey("should have ErrBackoff", func() {
						So(err, ShouldNotBeNil)
						So(errors.Is(err, ErrBackoff), ShouldBeTrue)
					})

					Convey("should still have 1 incoming Msgs", func() {
						So(s.SinkIncoming[key], ShouldHaveLength, 1)
					})
				})
			})
		})
	})
}

func TestStateSinkNak(t *testing.T) {
	now := time.Now()

	Convey("init state", t, func() {
		from := gateway.Deployment("xx")
		to := gateway.Deployment("yy")
		s, err := newState(to, from, testDefaultComm, nil)
		So(err, ShouldBeNil)

		Convey("register subscription, with this deployment as the sink", func() {
			req := &v1.StartSyncRequest{
				SourceDeployment: from.String(),
				SinkDeployment:   to.String(),
				SourceStreamName: "stream1",
				ConsumerConfig:   &v1.ConsumerConfig{DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL}}
			err := s.RegisterStartSync(req)
			So(err, ShouldBeNil)

			key := SinkSubscriptionKey{SourceStreamName: "stream1"}

			Convey("deliver a msg with last sequence>0", func() {

				msg := &v1.Msg{
					Subject:            "x.y.z",
					Data:               []byte("123"),
					Sequence:           31,
					PublishedTimestamp: timestamppb.Now()}

				msgs1 := &v1.Msgs{
					SetId:            NewSetID().String(),
					SourceDeployment: "xx",
					SinkDeployment:   "yy",
					SourceStreamName: "stream1",
					ConsumerConfig:   &v1.ConsumerConfig{DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL},
					LastSequence:     30,
					Messages:         []*v1.Msg{msg}}

				t0 := time.Now()
				err := s.SinkDeliverFromRemote(t0, msgs1)
				So(err, ShouldBeNil)

				Convey("should have 1 incoming Msgs", func() {
					So(s.SinkIncoming[key], ShouldHaveLength, 1)
				})

				Convey("reject set ID", func() {
					err := s.SinkCommitReject(msgs1, 0)
					So(err, ShouldBeNil)

					Convey("should not have incoming messages", func() {
						So(s.SinkIncoming[key], ShouldBeEmpty)
					})

					Convey("should have 1 pending ack", func() {
						So(s.PendingStats(now), ShouldResemble, []int{0, 0, 1, 0})

						b1, err := s.CreateMessageBatch(time.Now())
						So(err, ShouldBeNil)
						nak := b1.Acknowledges[0]

						Convey("should indicate nak", func() {
							So(nak.IsNegative, ShouldBeTrue)
						})
					})
				})
			})
		})
	})
}
