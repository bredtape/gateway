package sync

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/bredtape/gateway"
	rh "github.com/bredtape/gateway/remote_http/v1"
	v1 "github.com/bredtape/gateway/sync/v1"
	"github.com/bredtape/retry"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
)

// test scenarios

// low level sync service up and running:
// spin up 2 nats servers to represent deployment A and B
// start nats sync on each with file exchange for inter-communication
// create nats subscription stream on each
// publish subscription to sync the subscription stream itself from A to B

// create "app" stream on A, which are to be synced to B
// publish Subscription request to sync the "app" stream from A to B directly on both A and B
// publish messages to the "app" stream on A
// the messages should be synced to B

// with sync of request/reply stream:
// create a request and reply stream on each deployment for the NatsSyncService
// then
// * publish a Subscribe request to sync the request and reply for NatsSyncService
// * request stream info of A from A
// * request stream info of B from A

// with http request over nats:
// start http over nats service on each deployment
// publish a Subscribe request to sync the http over nats service
// then
// * issue a http request of some http endpoint at B from A (could simply be Prometheus
//   metrics from the NatsSyncService itself)
// * issue a http request for A from A

const (
	envNatsUrlsA      = "TEST_NATS_URLS_A"
	envNatsUrlsB      = "TEST_NATS_URLS_B"
	fallbackNatsUrlsA = "nats://localhost:4222"
	fallbackNatsUrlsB = "nats://localhost:4322"
)

func TestNatsSyncLowLevelSync(t *testing.T) {
	// spin up 2 nats servers to represent deployment A and B
	// start nats sync on each with file exchange for inter-communication
	// create nats subscription stream on each
	// publish subscription to sync the subscription stream itself from A to B

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	da := gateway.Deployment("A")
	db := gateway.Deployment("B")

	fecA := FileExchangeConfig{
		IncomingDir:          t.TempDir(),
		OutgoingDir:          t.TempDir(),
		PollingStartDelay:    0,
		PollingInterval:      100 * time.Millisecond,
		PollingRetryInterval: 100 * time.Millisecond}

	defaultCommSettings := CommunicationSettings{
		AckTimeoutPrSubscription:        time.Second,
		AckRetryPrSubscription:          retry.Must(retry.NewExp(0.2, 10*time.Millisecond, 500*time.Millisecond)),
		HeartbeatIntervalPrSubscription: 100 * time.Millisecond,
		MaxAccumulatedPayloadSizeBytes:  4 << 20, // 4 MB
		PendingAcksPrSubscriptionMax:    5}

	// deployment A
	var clientA *NatsSyncClient
	{
		js := getJSConnA()
		ex, err := NewFileExchange(fecA)
		assert.NoError(t, err)

		cs := defaultCommSettings
		config := NatsSyncConfig{
			Deployment:            da,
			SyncStream:            generateRandomString() + "subscription",
			CommunicationSettings: map[gateway.Deployment]CommunicationSettings{db: cs},
			Exchanges:             map[gateway.Deployment]Exchange{db: ex}}
		err = StartNatsSync(ctx, js, config)
		assert.NoError(t, err)

		clientA = NewSyncClient(config.SyncStream, js)
		s, err := clientA.CreateSyncStream(ctx)
		assert.NoError(t, err)

		info, err := s.Info(ctx)
		assert.NoError(t, err)
		t.Logf("created subscription stream on A: %+v", info)
	}

	// deployment B
	var clientB *NatsSyncClient
	{
		js := getJSConnB()
		fecB := fecA
		fecB.IncomingDir, fecB.OutgoingDir = fecA.OutgoingDir, fecA.IncomingDir // reverse dirs
		ex, err := NewFileExchange(fecB)
		assert.NoError(t, err)

		cs := defaultCommSettings
		config := NatsSyncConfig{
			Deployment:            db,
			SyncStream:            generateRandomString() + "subscription",
			CommunicationSettings: map[gateway.Deployment]CommunicationSettings{da: cs},
			Exchanges:             map[gateway.Deployment]Exchange{da: ex}}
		err = StartNatsSync(ctx, js, config)
		assert.NoError(t, err)

		clientB = NewSyncClient(config.SyncStream, js)
		s, err := clientB.CreateSyncStream(ctx)
		assert.NoError(t, err, "failed to create subscription stream on B")

		info, err := s.Info(ctx)
		assert.NoError(t, err)
		t.Logf("created subscription stream on B: %+v", info)
	}

	// publish subscription directly to each nats-servers, to enable sync of the subscription stream itself
	{
		ack, err := clientA.PublishBootstrapSync(ctx, da, db)
		assert.NoError(t, err)
		t.Logf("publish bootstrap at A, ack %+v", ack)

		ack, err = clientB.PublishBootstrapSync(ctx, da, db)
		assert.NoError(t, err)
		t.Logf("publish bootstrap at B, ack %+v", ack)
	}

	// create "app" stream on A, which are to be synced to B
	appStreamA := generateRandomString() + "_app"
	{
		jsA, err := clientA.js.Connect(ctx)
		assert.NoError(t, err)

		_, err = jsA.CreateStream(ctx, jetstream.StreamConfig{
			Name:        appStreamA,
			Description: "Stream for app messages",
			Subjects:    []string{appStreamA + ".*"}})
		assert.NoError(t, err)
		t.Logf("created app stream on A: %s", appStreamA)
	}

	// create "app" stream on B, which are to be synced from A
	appStreamB := generateRandomString() + "_app"
	{
		jsB, err := clientB.js.Connect(ctx)
		assert.NoError(t, err)

		_, err = jsB.CreateStream(ctx, jetstream.StreamConfig{
			Name:        appStreamB,
			Description: "Stream for app messages. Sync'ed from A",
			Subjects:    []string{appStreamB + ".*"}})
		assert.NoError(t, err)
		t.Logf("created app stream on B: %s", appStreamB)
	}

	// publish Subscription request to sync the "app" stream from A to B directly
	{
		sub := &v1.StartSyncRequest{
			SourceDeployment: da.String(),
			SinkDeployment:   db.String(),
			SourceStreamName: appStreamA}
		_, err := clientA.publishStartSyncRequest(ctx, sub)
		assert.NoError(t, err)

		_, err = clientB.publishStartSyncRequest(ctx, sub)
		assert.NoError(t, err)
		t.Logf("requested sync of app stream from A to B")
	}

	// publish messages to the "app" stream on A. Use http GetRequest (to have a defined proto message, but different)
	{
		req := &rh.GetRequest{Url: "http://something.com"}
		ack, err := clientA.js.PublishProto(ctx, appStreamA+".something", nil, req, jetstream.WithExpectStream(appStreamA))
		assert.NoError(t, err)
		t.Logf("published http request to %s at A", ack.Stream)
	}

	// wait for messages to be synced to B
	{
		js, err := clientB.js.Connect(ctx)
		assert.NoError(t, err)

		// deliver all
		c, err := js.OrderedConsumer(ctx, appStreamB, jetstream.OrderedConsumerConfig{})
		assert.NoError(t, err)

		t.Logf("waiting for messages to be synced to B")
		for {
			msg, err := c.Next()
			assert.NoError(t, err)

			t.Logf("received message: %s", msg)
		}
	}
}

func getJSConnA() *JSConn {
	natsUrls := os.Getenv(envNatsUrlsA)
	if natsUrls == "" {
		natsUrls = fallbackNatsUrlsA
	}
	js := NewJSConn(JSConfig{URLs: natsUrls})
	return js
}

func getJSConnB() *JSConn {
	natsUrls := os.Getenv(envNatsUrlsB)
	if natsUrls == "" {
		natsUrls = fallbackNatsUrlsB
	}
	js := NewJSConn(JSConfig{URLs: natsUrls})
	return js
}

func generateRandomString() string {
	return uuid.Must(uuid.NewRandom()).String()
}
