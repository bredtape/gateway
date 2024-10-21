package gateway

import (
	"errors"
	"fmt"
	"regexp"
)

var deploymentNameRegex = regexp.MustCompile(`^[a-z][a-z0-9]*$`)

type Deployment string

func (d Deployment) String() string {
	return string(d)
}

func (d *Deployment) UnmarshalText(text []byte) error {
	*d = Deployment(text)
	return nil
}

func (d Deployment) MarshalText() ([]byte, error) {
	return []byte(d), nil
}

func (d Deployment) Validate() error {
	if len(d) == 0 {
		return errors.New("empty deployment")
	}
	if !deploymentNameRegex.MatchString(d.String()) {
		return fmt.Errorf("invalid deployment name: %s", d.String())
	}
	return nil
}
