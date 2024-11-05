//go:build nats

package sync

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

/*
to replicate https://github.com/nats-io/nats.go/issues/1734
bug where OptStartTime is set but DeliverPolicy is not set to DeliverByStartTimePolicy

docker log:
[TRC] 172.21.0.1:54642 - cid:197 - <<- [PUB $JS.API.CONSUMER.CREATE.c415e8ead2704943a258f30bc180004c_jsconn_test.u4WPoF0Hd2Bst2DxUgjmZ1_2 _INBOX.u4WPoF0Hd2Bst2DxUgjmWK.Ew8O36VG 308]
[TRC] 172.21.0.1:54642 - cid:197 - <<- MSG_PAYLOAD: ["{\"stream_name\":\"c415e8ead2704943a258f30bc180004c_jsconn_test\",\"config\":{\"name\":\"u4WPoF0Hd2Bst2DxUgjmZ1_2\",\"deliver_policy\":\"by_start_time\",\"opt_start_time\":\"0001-01-01T00:00:00Z\",\"ack_policy\":\"none\",\"replay_policy\":\"instant\",\"inactive_threshold\":300000000000,\"num_replicas\":1,\"mem_storage\":true},\"action\":\"\"}"]
*/
func TestOrderedConsumerOptStartTimeOverride(t *testing.T) {
	ctx := context.Background()

	urls := "nats://localhost:4222"
	nc, err := nats.Connect(urls)
	if err != nil {
		t.Fatal(err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}

	streamConfig := jetstream.StreamConfig{
		Name:        "test",
		Description: "desc",
		Subjects:    []string{"test.*"}}

	_, err = js.CreateStream(ctx, streamConfig)
	if err != nil {
		t.Fatalf("failed to create stream: %v", err)
	}

	_, err = js.Publish(ctx, "test.1", []byte("hello"))
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}
	_, err = js.Publish(ctx, "test.2", []byte("hello"))
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	cfg := jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:   2,
		OptStartTime:  &time.Time{}}
	consumer, err := js.OrderedConsumer(ctx, "test", cfg)
	if err != nil {
		t.Fatalf("failed to create consumer: %v", err)
	}

	msg, err := consumer.Next()
	if err != nil {
		t.Fatalf("failed to get message: %v", err)
	}

	if msg.Subject() != "test.2" {
		t.Fatalf("expected subject to be test.2, got %s", msg.Subject())
	}
}
