package sync

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"

	v1 "github.com/bredtape/gateway/sync/v1"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

const headerFrom = "gateway-from"
const headerTo = "gateway-to"
const headerDigest = "digest" // header for digest of body, e.g.  Digest: sha-256=X48E9qOokqqrvdts8nOJRJN3OWDUoyWxBf7kbu9DBPE=
const receiveBuffer = 100

var (
	protobufMessageType_MessageBatch = string((&v1.MessageBatch{}).ProtoReflect().Descriptor().FullName())
)

type HTTPExchangeConfig struct {
	// this deployment. Only accept messages to this deployment
	From string

	// remote deployment. Will be added to headers
	To string

	// client URL to post outgoing MessageBatch'es
	ClientURL string
}

func (c HTTPExchangeConfig) Validate() error {
	if c.From == "" {
		return errors.New("from empty")
	}
	if c.To == "" {
		return errors.New("to empty")
	}
	if c.From == c.To {
		return errors.New("from and to must be different")
	}
	if c.ClientURL == "" {
		return errors.New("clientURL empty")
	}
	if _, err := url.Parse(c.ClientURL); err != nil {
		return errors.Wrap(err, "clientURL invalid")
	}
	return nil
}

// HTTPExchange expects incoming v1.MessageExhange to be posted to the hosted endpoint
// and acts as a client to the specified HTTP endpoint
// It must be registered as an http.Handler
type HTTPExchange struct {
	cfg    HTTPExchangeConfig
	out    chan *v1.MessageBatch
	client *http.Client
}

func NewHTTPExchange(cfg HTTPExchangeConfig) (*HTTPExchange, error) {
	return newHTTPExchange(cfg, http.DefaultClient)
}

func newHTTPExchange(cfg HTTPExchangeConfig, client *http.Client) (*HTTPExchange, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &HTTPExchange{
		cfg:    cfg,
		out:    make(chan *v1.MessageBatch, receiveBuffer),
		client: client}, nil
}

// implements http.Handler. Only accepts POST requests
func (ex *HTTPExchange) ServeHTTP(w http.ResponseWriter, r *http.Request) {

	ctx := r.Context()

	if r.Method != http.MethodPost {
		http.Error(w, "only POST allowed", http.StatusMethodNotAllowed)
		return
	}

	if r.Header.Get(headerContentType) != contentTypeProto {
		http.Error(w, headerContentType+" header must be "+contentTypeProto, http.StatusBadRequest)
		return
	}

	if r.Header.Get(headerProtoType) != protobufMessageType_MessageBatch {
		http.Error(w, headerProtoType+" header must be "+protobufMessageType_MessageBatch, http.StatusBadRequest)
		return
	}

	digest := r.Header.Get(headerDigest)
	if digest == "" {
		http.Error(w, headerDigest+" header must be set", http.StatusBadRequest)
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if !reDigestMatch.MatchString(digest) {
		http.Error(w, headerDigest+" header must be sha256 then base64 url encoded and specified as sha256=X<encoded value>. Received value "+digest, http.StatusBadRequest)
		return
	}
	if hashSHA256AndBase64(data) != digest {
		http.Error(w, "digest mismatch", http.StatusBadRequest)
	}

	var msg v1.MessageBatch
	err = proto.Unmarshal(data, &msg)
	if err != nil {
		http.Error(w, "failed to unmarshal message", http.StatusBadRequest)
		return
	}

	if msg.GetToDeployment() != ex.cfg.From {
		http.Error(w, "deployment not accepted", http.StatusBadRequest)
		return
	}

	select {
	case <-ctx.Done():
		http.Error(w, "context done", http.StatusRequestTimeout)
		return
	case ex.out <- &msg:
		w.WriteHeader(http.StatusOK)
	}
}

func (ex *HTTPExchange) StartReceiving(ctx context.Context) (<-chan *v1.MessageBatch, error) {
	return ex.out, nil
}

func (ex *HTTPExchange) Write(ctx context.Context, msg *v1.MessageBatch) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return errors.Wrap(err, "failed to marshal message")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ex.cfg.ClientURL, bytes.NewReader(data))
	if err != nil {
		return errors.Wrap(err, "failed to create request")
	}

	req.Header.Set(headerContentType, contentTypeProto)
	req.Header.Set(headerProtoType, protobufMessageType_MessageBatch)
	req.Header.Set(headerDigest, hashSHA256AndBase64(data))
	req.Header.Set(headerFrom, ex.cfg.From)
	req.Header.Set(headerTo, ex.cfg.To)

	resp, err := ex.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to send request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		slog.Debug("write failed", "module", "sync", "op", "HTTPExchange/Write", "status", resp.StatusCode, "body", string(body))
		return errors.Errorf("expected status OK; got status code %d", resp.StatusCode)
	}
	return nil
}

// match sha256=X<base64 url encoded value>
var reDigestMatch = regexp.MustCompile(`^sha256=X(?:[A-Za-z0-9-_]{4})*(?:[A-Za-z0-9-_]{2}==|[A-Za-z0-9-_]{3}=)?$`)
