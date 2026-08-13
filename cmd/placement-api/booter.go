package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pranav-gupta1/microvm-placement/internal/scheduler"
)

// addressResolver maps a host to the address its agent listens on.
type addressResolver interface {
	Address(id scheduler.HostID) (string, bool)
}

// httpBooter starts guests by calling the vmhostd agent on the chosen host.
//
// This is the step that makes a placement real. Without it the scheduler hands
// out slots that no agent ever hears about, so no guest is ever created, no TTL
// ever fires, and the fleet fills up once and never drains. That failure looks
// exactly like a capacity shortage from the outside, which is what makes it
// worth naming here.
type httpBooter struct {
	resolver addressResolver
	client   *http.Client
}

func newHTTPBooter(resolver addressResolver, timeout time.Duration, maxConns int) *httpBooter {
	return &httpBooter{
		resolver: resolver,
		client: &http.Client{
			Timeout: timeout,
			// The default transport keeps only two idle connections per host.
			// With dozens of agents and a thousand boots a second, that would
			// mean a new TCP handshake for most requests.
			Transport: &http.Transport{
				MaxIdleConns:        maxConns,
				MaxIdleConnsPerHost: 16,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func (b *httpBooter) Boot(ctx context.Context, host scheduler.HostID, vmID string, ttl time.Duration) error {
	addr, ok := b.resolver.Address(host)
	if !ok {
		return fmt.Errorf("no address known for host %s", host)
	}

	body, err := json.Marshal(map[string]any{"id": vmID, "ttl_ms": ttl.Milliseconds()})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/vms", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := b.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	// Drain so the connection can be reused rather than torn down.
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusCreated {
		// 409 means the agent is full, so the scheduler's view of it was
		// stale. The caller retries on another host.
		return fmt.Errorf("agent %s returned %s", addr, resp.Status)
	}
	return nil
}
