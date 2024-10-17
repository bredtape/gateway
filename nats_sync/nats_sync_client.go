package nats_sync

import (
	"context"
	"fmt"

	"github.com/bredtape/gateway"
	v1 "github.com/bredtape/gateway/nats_sync/v1"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
)

type NatsSyncClient struct {
	syncStream string
	js         *JSConn
}

func NewSyncClient(syncStream string, js *JSConn) *NatsSyncClient {
	return &NatsSyncClient{
		syncStream: syncStream,
		js:         js,
	}
}

// create nats stream for sync stream. Should only be used for testing
func (client *NatsSyncClient) CreateSyncStream(ctx context.Context) (jetstream.Stream, error) {
	c, err := client.js.Connect(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to nats")
	}

	return c.CreateStream(ctx, jetstream.StreamConfig{
		Name:              client.syncStream,
		Description:       "Stream for persisting Start/Stop sync requests. Must exist at all 'deployments' participating in the sync. Subjects: <sink deployment>.<source_deployment>.<source stream name>",
		Subjects:          []string{client.syncStream + ".*.*.*"},
		Retention:         jetstream.LimitsPolicy,
		MaxMsgsPerSubject: 5,
		Discard:           jetstream.DiscardOld})
}

// publish Sync request for the sync itself from source to sink deployment.
// The same request must be published at both the source and sink deployment.
func (client *NatsSyncClient) PublishBootstrapSync(ctx context.Context, source, sink gateway.Deployment) (*jetstream.PubAck, error) {
	req := &v1.StartSyncRequest{
		SourceDeployment: source.String(),
		SinkDeployment:   sink.String(),
		SourceStreamName: subscriptionStream, // use placeholder
		SinkStreamName:   subscriptionStream, // use placeholder
		FilterSubjects:   nil,
		ConsumerConfig: &v1.ConsumerConfig{
			DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL}}

	return client.publishStartSyncRequest(ctx, req)
}

func (client *NatsSyncClient) publishStartSyncRequest(ctx context.Context, req *v1.StartSyncRequest) (*jetstream.PubAck, error) {
	subject := fmt.Sprintf("%s.%s.%s.%s", client.syncStream,
		req.SinkDeployment, req.SourceDeployment, req.SourceStreamName)

	return client.js.PublishProto(ctx, subject, nil, req, jetstream.WithExpectStream(client.syncStream))
}

func (client *NatsSyncClient) publishStopSyncRequest(ctx context.Context, req *v1.StopSyncRequest) (*jetstream.PubAck, error) {
	subject := fmt.Sprintf("%s.%s.%s.%s", client.syncStream,
		req.SinkDeployment, req.SourceDeployment, req.SourceStreamName)

	return client.js.PublishProto(ctx, subject, nil, req, jetstream.WithExpectStream(client.syncStream))
}
