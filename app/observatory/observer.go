package observatory

import (
	"context"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/common/signal/done"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/extension"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
	"google.golang.org/protobuf/proto"
)

type Observer struct {
	config *Config
	ctx    context.Context

	statusLock sync.Mutex
	status     []*OutboundStatus

	finished *done.Instance

	ohm        outbound.Manager
	dispatcher routing.Dispatcher
}

func (o *Observer) GetObservation(ctx context.Context) (proto.Message, error) {
	o.statusLock.Lock()
	defer o.statusLock.Unlock()
	return proto.Clone(&ObservationResult{Status: o.status}), nil
}

func (o *Observer) Type() interface{} {
	return extension.ObservatoryType()
}

func (o *Observer) Start() error {
	if o.config != nil && len(o.config.SubjectSelector) != 0 {
		o.finished = done.New()
		go o.background()
	}
	return nil
}

func (o *Observer) Close() error {
	if o.finished != nil {
		return o.finished.Close()
	}
	return nil
}

func (o *Observer) background() {
	for !o.finished.Done() {
		hs, ok := o.ohm.(outbound.HandlerSelector)
		if !ok {
			errors.LogInfo(o.ctx, "outbound.Manager is not a HandlerSelector")
			return
		}

		outbounds := hs.Select(o.config.SubjectSelector)

		o.clearRemovedOutbounds(outbounds)

		sleepTime := time.Second * 10
		if o.config.ProbeInterval != 0 {
			sleepTime = time.Duration(o.config.ProbeInterval)
		}

		if len(outbounds) == 0 {
			errors.LogWarning(o.ctx, "no outbound matches subjectSelector ", o.config.SubjectSelector)
			time.Sleep(sleepTime)
			continue
		}

		if !o.config.EnableConcurrency {
			sort.Strings(outbounds)
			for _, v := range outbounds {
				result := o.probe(v)
				o.updateStatusForResult(v, &result)
				if o.finished.Done() {
					return
				}
				time.Sleep(sleepTime)
			}
			continue
		}

		ch := make(chan struct{}, len(outbounds))

		for _, v := range outbounds {
			go func(v string) {
				result := o.probe(v)
				o.updateStatusForResult(v, &result)
				ch <- struct{}{}
			}(v)
		}

		for range outbounds {
			select {
			case <-ch:
			case <-o.finished.Wait():
				return
			}
		}
		time.Sleep(sleepTime)
	}
}

func (o *Observer) clearRemovedOutbounds(outbounds []string) {
	o.statusLock.Lock()
	defer o.statusLock.Unlock()
	if len(o.status) == 0 {
		return
	}
	var pruned []*OutboundStatus
	for _, status := range o.status {
		if slices.Contains(outbounds, status.OutboundTag) {
			pruned = append(pruned, status)
		}
	}
	o.status = pruned
}

func (o *Observer) probe(outbound string) ProbeResult {
	errorCollectorForRequest := newErrorCollector()
	ctx := session.TrackedConnectionError(o.ctx, errorCollectorForRequest)
	probeURL := "https://www.google.com/generate_204"
	if o.config.ProbeUrl != "" {
		probeURL = o.config.ProbeUrl
	}
	// Stock Observatory considers any completed HTTP response successful.
	result := probeHTTP(ctx, o.dispatcher, outbound, probeURL, 5*time.Second, false)
	if !result.Alive {
		result.LastErrorReason = "the outbound " + outbound + " is dead: GET request failed:" + result.LastErrorReason + "with outbound handler report underlying connection failed"
		errors.LogInfoInner(o.ctx, errorCollectorForRequest.UnderlyingError(), result.LastErrorReason)
		return result
	}
	errors.LogInfo(o.ctx, "the outbound ", outbound, " is alive:", float64(result.Delay)/1000)
	return result
}

func (o *Observer) updateStatusForResult(outbound string, result *ProbeResult) {
	o.statusLock.Lock()
	defer o.statusLock.Unlock()
	var status *OutboundStatus
	if location := o.findStatusLocationLockHolderOnly(outbound); location != -1 {
		status = o.status[location]
	} else {
		status = &OutboundStatus{}
		o.status = append(o.status, status)
	}

	status.LastTryTime = time.Now().Unix()
	status.OutboundTag = outbound
	status.Alive = result.Alive
	if result.Alive {
		status.Delay = result.Delay
		status.LastSeenTime = status.LastTryTime
		status.LastErrorReason = ""
	} else {
		status.LastErrorReason = result.LastErrorReason
		status.Delay = 99999999
	}
}

func (o *Observer) findStatusLocationLockHolderOnly(outbound string) int {
	for i, v := range o.status {
		if v.OutboundTag == outbound {
			return i
		}
	}
	return -1
}

func New(ctx context.Context, config *Config) (*Observer, error) {
	var outboundManager outbound.Manager
	var dispatcher routing.Dispatcher
	err := core.RequireFeatures(ctx, func(om outbound.Manager, rd routing.Dispatcher) {
		outboundManager = om
		dispatcher = rd
	})
	if err != nil {
		return nil, errors.New("Cannot get depended features").Base(err)
	}
	return &Observer{
		config:     config,
		ctx:        ctx,
		ohm:        outboundManager,
		dispatcher: dispatcher,
	}, nil
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		return New(ctx, config.(*Config))
	}))
}
