package nats_sync

import (
	"time"

	"github.com/bredtape/gateway"
	v1 "github.com/bredtape/gateway/nats_sync/v1"
	"github.com/pkg/errors"
)

func (s *state) TargetDeliverFromRemote(t time.Time, msgs *v1.Msgs) error {
	target := gateway.Deployment(msgs.GetTargetDeployment())
	if target != s.deployment {
		return errors.New("target deployment does not match")
	}

	seq := RangeInclusive[uint64]{
		From: msgs.GetSequenceFrom(),
		To:   msgs.GetSequenceTo()}

}
