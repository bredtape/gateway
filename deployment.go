package gateway

// deployment name. Allowed characters: [a-z0-9]
type Deployment string

func (d Deployment) String() string {
	return string(d)
}
