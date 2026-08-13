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
type httpBooter struct {
	resolver addressResolver
	client   *http.Client
}

func newHTTPBooter(resolver addressResolver, timeout time.Duration, maxConns int) *httpBooter {
	return &httpBooter{
		resolver: resolver,
		client: &http.Client{
			Timeout: timeout,
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
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("agent %s returned %s", addr, resp.Status)
	}
	return nil
}
