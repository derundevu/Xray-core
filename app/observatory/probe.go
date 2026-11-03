package observatory

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/xtls/xray-core/common/errors"
	v2net "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/common/utils"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport/internet/tagged"
)

// Probe performs one bounded HTTP GET through exactly outboundTag. The context
// must belong to the Xray instance containing dispatcher and its outbound manager.
// Missing tags, redirects and non-2xx responses fail; no direct dial is attempted.
// Cancellation applies to the request and dispatched connection while preserving
// Xray context values. The caller owns scheduling and interpretation of results.
func Probe(ctx context.Context, dispatcher routing.Dispatcher, outboundTag, probeURL string, timeout time.Duration) ProbeResult {
	if outboundTag == "" {
		return ProbeResult{LastErrorReason: "probe outbound tag is empty"}
	}
	if timeout <= 0 {
		return ProbeResult{LastErrorReason: "probe timeout must be positive"}
	}
	if dispatcher == nil || tagged.Dialer == nil {
		return ProbeResult{LastErrorReason: "tagged probe dispatcher is unavailable"}
	}
	instance := core.FromContext(ctx)
	if instance == nil {
		return ProbeResult{LastErrorReason: "probe requires an Xray instance context"}
	}
	manager, ok := instance.GetFeature(outbound.ManagerType()).(outbound.Manager)
	if !ok {
		return ProbeResult{LastErrorReason: "probe outbound manager is unavailable"}
	}
	if handler := manager.GetHandler(outboundTag); handler == nil || handler.Tag() != outboundTag {
		return ProbeResult{LastErrorReason: "probe outbound handler does not exist: " + outboundTag}
	}
	// The dispatcher also rejects a forced tag removed after this lookup, rather
	// than falling back to normal routing or the default outbound.
	return probeHTTP(session.ContextWithCancellationPropagation(ctx, true), dispatcher, outboundTag, probeURL, timeout, true)
}

// probeHTTP shares Observatory's tagged HTTP transport with the managed probe.
// require2xx deliberately does not change stock Observatory's response semantics.
func probeHTTP(ctx context.Context, dispatcher routing.Dispatcher, outboundTag, probeURL string, timeout time.Duration, require2xx bool) ProbeResult {
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	request, err := http.NewRequestWithContext(probeCtx, http.MethodGet, probeURL, nil)
	if err != nil {
		return ProbeResult{LastErrorReason: errors.New("cannot create probe request").Base(err).Error()}
	}
	utils.TryDefaultHeadersWith(request.Header, "nav")

	httpTransport := &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(_ context.Context, network, addr string) (net.Conn, error) {
			dest, err := v2net.ParseDestination(network + ":" + addr)
			if err != nil {
				return nil, errors.New("cannot understand address").Base(err)
			}
			// net/http may detach its dial context from request cancellation. Use
			// the bounded request context, including the original Xray values, so
			// an outbound cannot outlive this one-shot probe.
			conn, err := tagged.Dialer(probeCtx, dispatcher, dest, outboundTag)
			if err != nil {
				return nil, errors.New("cannot dial remote address ", dest).Base(err)
			}
			return conn, nil
		},
		TLSHandshakeTimeout:   timeout,
		ResponseHeaderTimeout: timeout,
	}
	defer httpTransport.CloseIdleConnections()
	httpClient := &http.Client{
		Transport: httpTransport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	start := time.Now()
	response, err := httpClient.Do(request)
	if err != nil {
		return ProbeResult{LastErrorReason: errors.New("outbound failed to relay connection").Base(err).Error()}
	}
	defer response.Body.Close()
	if require2xx && (response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices) {
		return ProbeResult{LastErrorReason: "probe returned HTTP status " + response.Status}
	}
	return ProbeResult{Alive: true, Delay: time.Since(start).Milliseconds()}
}
