package outbound_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/xtls/xray-core/app/policy"
	"github.com/xtls/xray-core/app/proxyman"
	. "github.com/xtls/xray-core/app/proxyman/outbound"
	"github.com/xtls/xray-core/app/stats"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/session"
	core "github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/proxy/freedom"
	"github.com/xtls/xray-core/transport/internet/stat"
)

func TestInterfaces(t *testing.T) {
	_ = outbound.Handler(new(Handler))
	_ = outbound.Manager(new(Manager))
}

const xrayKey core.XrayKey = 1

func TestOutboundWithoutStatCounter(t *testing.T) {
	config := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&stats.Config{}),
			serial.ToTypedMessage(&policy.Config{
				System: &policy.SystemPolicy{
					Stats: &policy.SystemPolicy_Stats{
						InboundUplink: true,
					},
				},
			}),
		},
	}

	v, _ := core.New(config)
	v.AddFeature(outbound.Manager(new(Manager)))
	ctx := context.WithValue(context.Background(), xrayKey, v)
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{}})
	h, _ := NewHandler(ctx, &core.OutboundHandlerConfig{
		Tag:           "tag",
		ProxySettings: serial.ToTypedMessage(&freedom.Config{FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}}}),
	})
	conn, _ := h.(*Handler).Dial(ctx, net.TCPDestination(net.DomainAddress("localhost"), 13146))
	_, ok := conn.(*stat.CounterConnection)
	if ok {
		t.Errorf("Expected conn to not be CounterConnection")
	}
}

func TestOutboundWithStatCounter(t *testing.T) {
	config := &core.Config{
		App: []*serial.TypedMessage{
			serial.ToTypedMessage(&stats.Config{}),
			serial.ToTypedMessage(&policy.Config{
				System: &policy.SystemPolicy{
					Stats: &policy.SystemPolicy_Stats{
						OutboundUplink:   true,
						OutboundDownlink: true,
					},
				},
			}),
		},
	}

	v, _ := core.New(config)
	v.AddFeature(outbound.Manager(new(Manager)))
	ctx := context.WithValue(context.Background(), xrayKey, v)
	ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{}})
	h, _ := NewHandler(ctx, &core.OutboundHandlerConfig{
		Tag:           "tag",
		ProxySettings: serial.ToTypedMessage(&freedom.Config{FinalRules: []*freedom.FinalRuleConfig{{Action: freedom.RuleAction_Allow}}}),
	})
	conn, _ := h.(*Handler).Dial(ctx, net.TCPDestination(net.DomainAddress("localhost"), 13146))
	_, ok := conn.(*stat.CounterConnection)
	if !ok {
		t.Errorf("Expected conn to be CounterConnection")
	}
}

type selectorTestHandler struct {
	outbound.Handler
	tag string
}

func (h *selectorTestHandler) Tag() string { return h.tag }

func TestSelectionTracksAddedAndRemovedOutboundsDuringConcurrentReads(t *testing.T) {
	manager, err := New(context.Background(), &proxyman.OutboundConfig{})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := manager.AddHandler(ctx, &selectorTestHandler{tag: "node.anchor"}); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var readers sync.WaitGroup
	for range 4 {
		readers.Go(func() {
			for {
				select {
				case <-done:
					return
				default:
				}
				for _, tag := range manager.Select([]string{"node."}) {
					if !strings.HasPrefix(tag, "node.") {
						t.Errorf("unrelated outbound selected: %q", tag)
						return
					}
				}
			}
		})
	}
	defer func() {
		close(done)
		readers.Wait()
	}()
	for i := range 1000 {
		tag := fmt.Sprintf("node.%d", i)
		if err := manager.AddHandler(ctx, &selectorTestHandler{tag: tag}); err != nil {
			t.Fatal(err)
		}
		current := manager.Select([]string{"node."})
		if len(current) != 2 || (current[0] != tag && current[1] != tag) {
			t.Fatalf("added outbound unavailable to balancer: %v", current)
		}
		if err := manager.RemoveHandler(ctx, tag); err != nil {
			t.Fatal(err)
		}
		current = manager.Select([]string{"node."})
		if len(current) != 1 || current[0] != "node.anchor" {
			t.Fatalf("removed outbound remains selectable: %v", current)
		}
	}
}
