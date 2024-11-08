package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/bredtape/gateway/sync"
	v1 "github.com/bredtape/gateway/sync/v1"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
	"google.golang.org/protobuf/proto"
)

/*
global options:
  --server     # nats server. Required
  --seed-file  # nats .nk seed file. Optional

command tree:
  bootstrap
	  create            # create sync stream
		sync [from] [to]  # publish sync request

	sync
	  start [from] [to] [source stream name] # start sync with deliver policy all, no subject filters
		stop  [from] [to] [source stream name] # stop sync
		ls [from] [to] # list source stream names being sync'ed
		info [from] [to] [source stream name] # get sync info

NOT IMPLEMENTED:
	stream
	  info [stream name] # get stream info
*/

func main() {
	app := &cli.App{
		Name:  "sync cli",
		Usage: "For easier administration of nats sync",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "server",
				Usage:    "Nats server",
				Required: true,
			},
			&cli.StringFlag{
				Name:  "seed-file",
				Usage: "Nats .nk seed file",
			},
			&cli.StringFlag{
				Name:  "sync_stream",
				Usage: "sync stream name (at this location)",
				Value: sync.SyncStreamPlaceholder,
			},
		},
		Commands: []*cli.Command{
			{
				Name:  "bootstrap",
				Usage: "create 'sync' stream and publish sync request",
				Subcommands: []*cli.Command{
					{
						Name:  "create",
						Usage: "create sync stream in nats",
						Action: func(c *cli.Context) error {
							js, err := getJS(c)
							if err != nil {
								return err
							}

							syncStreamName, err := getSyncStreamName(c)
							if err != nil {
								return err
							}
							return bootstrapCreate(c.Context, js, syncStreamName)
						},
					},
					{
						Name:        "sync",
						Usage:       "sync [from] [to]",
						Description: "publish sync request (for the sync itself). The SAME request must be published at both the source and sink deployment",
						Action: func(c *cli.Context) error {
							js, err := getJS(c)
							if err != nil {
								return err
							}

							syncStreamName, err := getSyncStreamName(c)
							if err != nil {
								return err
							}

							from := c.Args().Get(0)
							to := c.Args().Get(1)
							if from == "" || to == "" {
								return errors.New("from and to are required")
							}

							return bootstrapSync(c.Context, js, syncStreamName, from, to)
						},
					},
				},
			},
			{
				Name:        "sync",
				Usage:       "start or stop sync of a stream",
				Description: "Publish start/stop sync request of a stream",
				Subcommands: []*cli.Command{
					{
						Name:        "start",
						Usage:       "start [from] [to] [source stream name]",
						Description: "Publish start sync request with deliver policy all, no subject filters",
						Action: func(c *cli.Context) error {
							js, err := getJS(c)
							if err != nil {
								return err
							}

							syncStreamName, err := getSyncStreamName(c)
							if err != nil {
								return err
							}

							from := c.Args().Get(0)
							to := c.Args().Get(1)
							streamName := c.Args().Get(2)
							if from == "" || to == "" || streamName == "" {
								return errors.New("from, to  and stream name are required")
							}

							return syncStart(c.Context, js, syncStreamName, from, to, streamName)
						},
					},
					{
						Name:  "stop",
						Usage: "stop [from] [to] [source stream name]",
						Action: func(c *cli.Context) error {
							js, err := getJS(c)
							if err != nil {
								return err
							}

							syncStreamName, err := getSyncStreamName(c)
							if err != nil {
								return err
							}

							from := c.Args().Get(0)
							to := c.Args().Get(1)
							streamName := c.Args().Get(2)
							if from == "" || to == "" || streamName == "" {
								return errors.New("from, to  and stream name are required")
							}

							return syncStop(c.Context, js, syncStreamName, from, to, streamName)
						},
					},
					{
						Name:  "ls",
						Usage: "ls [from] [to]",
						Action: func(c *cli.Context) error {
							js, err := getJS(c)
							if err != nil {
								return err
							}

							syncStreamName, err := getSyncStreamName(c)
							if err != nil {
								return err
							}

							from := c.Args().Get(0)
							to := c.Args().Get(1)
							if from == "" || to == "" {
								return errors.New("from and to are required")
							}

							xs, err := syncList(c.Context, js, syncStreamName, from, to)
							if err != nil {
								return err
							}

							fmt.Fprintf(os.Stdout, "%v\n", xs)
							return nil
						},
					},
					{
						Name:  "info",
						Usage: "info [from] [to] [sourceStreamName]",
						Action: func(c *cli.Context) error {
							js, err := getJS(c)
							if err != nil {
								return err
							}

							syncStreamName, err := getSyncStreamName(c)
							if err != nil {
								return err
							}

							from := c.Args().Get(0)
							to := c.Args().Get(1)
							streamName := c.Args().Get(2)
							if from == "" || to == "" || streamName == "" {
								return errors.New("from, to and stream name are required")
							}
							xs, err := syncInfo(c.Context, js, syncStreamName, from, to, streamName)
							if err != nil {
								return err
							}

							fmt.Fprintf(os.Stdout, "%v\n", xs)
							return nil
						},
					},
				},
			},
		}}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}
}

func bootstrapCreate(ctx context.Context, js *sync.JSConn, syncStreamName string) error {
	return js.CreateStream(ctx, jetstream.StreamConfig{
		Name:              syncStreamName,
		Description:       "Stream for persisting Start/Stop sync requests. Must exist at all 'deployments' participating in the sync. Subjects: <sink deployment>.<source_deployment>.<source stream name>.A Start request must exist for the sync stream itself",
		Subjects:          []string{syncStreamName + ".*.*.*"},
		Retention:         jetstream.LimitsPolicy,
		Storage:           jetstream.FileStorage,
		MaxMsgsPerSubject: 10})
}

func bootstrapSync(ctx context.Context, js *sync.JSConn, syncStreamName string, from, to string) error {
	req := &v1.StartSyncRequest{
		SourceDeployment: from,
		SinkDeployment:   to,
		SourceStreamName: sync.SyncStreamPlaceholder,
		FilterSubjects:   nil,
		ConsumerConfig: &v1.ConsumerConfig{
			DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL}}

	subject := fmt.Sprintf("%s.%s.%s.%s", syncStreamName, to, from, sync.SyncStreamPlaceholder)
	_, err := js.PublishProto(ctx, subject, nil, req, jetstream.WithExpectStream(syncStreamName))
	return err
}

func syncStart(ctx context.Context, js *sync.JSConn, syncStreamName, from, to, streamName string) error {
	req := &v1.StartSyncRequest{
		SourceDeployment: from,
		SinkDeployment:   to,
		SourceStreamName: streamName,
		FilterSubjects:   nil,
		ConsumerConfig: &v1.ConsumerConfig{
			DeliverPolicy: v1.DeliverPolicy_DELIVER_POLICY_ALL}}

	subject := fmt.Sprintf("%s.%s.%s.%s", syncStreamName, to, from, streamName)
	_, err := js.PublishProto(ctx, subject, nil, req, jetstream.WithExpectStream(syncStreamName))
	return err
}

func syncStop(ctx context.Context, js *sync.JSConn, syncStreamName, from, to, streamName string) error {
	req := &v1.StopSyncRequest{
		SourceDeployment: from,
		SinkDeployment:   to,
		SourceStreamName: sync.SyncStreamPlaceholder,
		FilterSubjects:   nil}

	subject := fmt.Sprintf("%s.%s.%s.%s", syncStreamName, to, from, streamName)
	_, err := js.PublishProto(ctx, subject, nil, req, jetstream.WithExpectStream(syncStreamName))
	return err
}

func syncList(ctx context.Context, js *sync.JSConn, syncStreamName, from, to string) ([]string, error) {
	head, err := js.GetLastSequence(ctx, syncStreamName)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get last sequence")
	}

	var xs []string
	ch := js.StartSubscribeOrderered(ctx, syncStreamName, jetstream.OrderedConsumerConfig{DeliverPolicy: jetstream.DeliverLastPerSubjectPolicy})
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return xs, errors.New("did not receive all messages")
			}
			if strings.Contains(msg.Subject, fmt.Sprintf(".%s.%s.", to, from)) {
				xs = append(xs, msg.Subject)
			}

			if msg.Sequence == head {
				return xs, nil
			}
		}
	}
}

func syncInfo(ctx context.Context, js *sync.JSConn, syncStreamName, from, to, sourceStreamName string) ([]*v1.StartSyncRequest, error) {
	head, err := js.GetLastSequence(ctx, syncStreamName)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get last sequence")
	}

	var xs []*v1.StartSyncRequest
	ch := js.StartSubscribeOrderered(ctx, syncStreamName, jetstream.OrderedConsumerConfig{DeliverPolicy: jetstream.DeliverLastPerSubjectPolicy})
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return xs, errors.New("did not receive all messages")
			}
			if strings.Contains(msg.Subject, fmt.Sprintf(".%s.%s.%s", to, from, sourceStreamName)) {
				var x v1.StartSyncRequest
				err = proto.Unmarshal(msg.Data, &x)
				if err != nil {
					return nil, errors.Wrap(err, "failed to unmarshal message")
				}

				xs = append(xs, &x)
			}

			if msg.Sequence == head {
				return xs, nil
			}
		}
	}
}

func getJS(c *cli.Context) (*sync.JSConn, error) {
	server := c.String("server")
	if server == "" {
		return nil, fmt.Errorf("server is required")
	}
	cfg := sync.JSConfig{URLs: "nats://" + server}
	seedFile := c.String("seed-file")
	if len(seedFile) > 0 {
		err := cfg.WithSeedFile(seedFile)
		if err != nil {
			return nil, errors.Wrap(err, "failed to configure with seed file")
		}
	}
	return sync.NewJSConn(cfg), nil
}

func getSyncStreamName(c *cli.Context) (string, error) {
	s := c.String("sync_stream")
	if s == "" {
		return "", errors.New("sync stream name is required")
	}
	return s, nil
}
