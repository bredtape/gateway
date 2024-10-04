package nats_sync

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/bredtape/gateway"
	"github.com/bredtape/retry"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// test scenarios

// low level sync service up and running:
// spin up 2 nats servers to represent deployment A and B
// start nats sync on each with file exchange for inter-communication
// create nats subscription stream on each
// publish subscription to sync the subscription stream itself from A to B

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
		NakBackoffPrSubscription:        retry.Must(retry.NewExp(0.2, 10*time.Millisecond, 500*time.Millisecond)),
		FlushIntervalPrSubscription:     10 * time.Millisecond,
		HeartbeatIntervalPrSubscription: 100 * time.Millisecond,
		MaxAccumulatedPayloadSizeBytes:  4 << 20, // 4 MB
		MaxPendingAcksPrSubscription:    5}

	// deployment A
	var syncA *NatsSync
	{
		js := getJSConnA()
		ex, err := NewFileExchange(fecA)
		assert.NoError(t, err)

		cs := defaultCommSettings
		cs.Exchange = ex
		config := NatsSyncConfig{
			Deployment:            da,
			SubscriptionStream:    generateRandomString() + "subscription",
			CommunicationSettings: map[gateway.Deployment]CommunicationSettings{db: cs}}
		sync, err := NewNatsSync(js, config)
		assert.NoError(t, err)
		syncA = sync

		s, err := sync.CreateSubscriptionStream(ctx)
		assert.NoError(t, err)

		info, err := s.Info(ctx)
		assert.NoError(t, err)
		t.Logf("created subscription stream on A: %+v", info)
	}

	// deployment B
	var syncB *NatsSync
	{
		js := getJSConnB()
		fecB := fecA
		fecB.IncomingDir, fecB.OutgoingDir = fecA.OutgoingDir, fecA.IncomingDir // reverse dirs
		ex, err := NewFileExchange(fecB)
		assert.NoError(t, err)

		cs := defaultCommSettings
		cs.Exchange = ex
		config := NatsSyncConfig{
			Deployment:            db,
			SubscriptionStream:    generateRandomString() + "subscription",
			CommunicationSettings: map[gateway.Deployment]CommunicationSettings{da: cs}}
		sync, err := NewNatsSync(js, config)
		assert.NoError(t, err)
		syncB = sync

		s, err := sync.CreateSubscriptionStream(ctx)
		assert.NoError(t, err, "failed to create subscription stream on B")

		info, err := s.Info(ctx)
		assert.NoError(t, err)
		t.Logf("created subscription stream on B: %+v", info)
	}

	// publish subscription directly to nats to enable sync of the subscription stream itself
	{
		ack, err := syncA.PublishBootstrapSubscription(ctx, da, db)
		assert.NoError(t, err)
		t.Logf("publish bootstrap at A, ack %+v", ack)

		ack, err = syncB.PublishBootstrapSubscription(ctx, da, db)
		assert.NoError(t, err)
		t.Logf("publish bootstrap at B, ack %+v", ack)
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
