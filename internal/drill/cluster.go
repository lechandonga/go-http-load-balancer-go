package drill

import (
	"context"
	"fmt"
	"net/url"
)

// Cluster is a set of mock backends started together by the drill switch.
type Cluster struct {
	Backends []*Backend
}

// StartCluster launches count instances. ports may be nil (free ports are
// picked); if non-empty it must have exactly count entries.
func StartCluster(count int, ports []int, namePrefix string) (*Cluster, error) {
	if count <= 0 {
		return nil, fmt.Errorf("drill backend count must be > 0, got %d", count)
	}
	if len(ports) > 0 && len(ports) != count {
		return nil, fmt.Errorf("provided %d ports for %d backends", len(ports), count)
	}
	c := &Cluster{Backends: make([]*Backend, 0, count)}
	for i := 0; i < count; i++ {
		port := 0
		if len(ports) > 0 {
			port = ports[i]
		}
		b, err := Start(fmt.Sprintf("%s%d", namePrefix, i+1), port)
		if err != nil {
			_ = c.Shutdown(context.Background())
			return nil, fmt.Errorf("start mock backend %d: %w", i+1, err)
		}
		c.Backends = append(c.Backends, b)
	}
	return c, nil
}

// Targets returns gateway-ready (name, url) pairs for the cluster members.
func (c *Cluster) Targets() [][2]string {
	out := make([][2]string, len(c.Backends))
	for i, b := range c.Backends {
		u, _ := url.Parse(b.URL)
		out[i] = [2]string{b.Name, u.String()}
	}
	return out
}

// Shutdown stops every instance; the first error is returned.
func (c *Cluster) Shutdown(ctx context.Context) error {
	var firstErr error
	for _, b := range c.Backends {
		if err := b.Shutdown(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
