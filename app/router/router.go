package router

import (
	"context"
	"maps"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/dns"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
	routing_dns "github.com/xtls/xray-core/features/routing/dns"
)

// Router is an implementation of routing.Router.
type Router struct {
	domainStrategy Config_DomainStrategy
	rules          atomic.Pointer[[]*Rule]
	script         *scriptEngine
	balancers      atomic.Pointer[map[string]*Balancer]
	dns            dns.Client

	ctx        context.Context
	ohm        outbound.Manager
	dispatcher routing.Dispatcher
	mu         sync.Mutex
}

// Route is an implementation of routing.Route.
type Route struct {
	routing.Context
	outboundGroupTags []string
	outboundTag       string
	ruleTag           string
}

// Init initializes the Router.
func (r *Router) Init(ctx context.Context, config *Config, d dns.Client, ohm outbound.Manager, dispatcher routing.Dispatcher) error {
	r.domainStrategy = config.DomainStrategy
	r.dns = d
	r.ctx = ctx
	r.ohm = ohm
	r.dispatcher = dispatcher

	if config.Script != "" {
		script, err := newScriptEngine(config.Script)
		if err != nil {
			return errors.New("failed to prepare routing script").Base(err)
		}
		r.script = script
	}

	r.rules.Store(new([]*Rule))
	r.balancers.Store(&map[string]*Balancer{})
	return r.ReloadRules(config, false)
}

// PickRoute implements routing.Router.
func (r *Router) PickRoute(ctx routing.Context) (routing.Route, error) {
	if r.script != nil {
		return r.script.pickRoute(ctx)
	}

	originalCtx := ctx
	rule, ctx, err := r.pickRouteInternal(ctx)
	if err != nil {
		return nil, err
	}
	tag, err := rule.GetTag()
	if err != nil {
		return nil, err
	}
	if rule.Webhook != nil {
		rule.Webhook.Fire(originalCtx, tag)
	}
	return &Route{Context: ctx, outboundTag: tag, ruleTag: rule.RuleTag}, nil
}

// AddRule implements routing.Router.
func (r *Router) AddRule(config *serial.TypedMessage, shouldAppend bool) error {
	inst, err := config.GetInstance()
	if err != nil {
		return err
	}
	if c, ok := inst.(*Config); ok {
		return r.ReloadRules(c, shouldAppend)
	}
	return errors.New("AddRule: config type error")
}

func (r *Router) ReloadRules(config *Config, shouldAppend bool) error {
	return r.reloadRules(config, shouldAppend, "", nil)
}

// ReloadRulesWithFallback replaces the complete routing snapshot, including the
// outbound used when no rule matches. Unlike a catch-all configured rule, this
// fallback does not suppress the IPIfNonMatch DNS retry.
func (r *Router) ReloadRulesWithFallback(config *Config, fallbackTag string) error {
	return r.reloadRules(config, false, fallbackTag, nil)
}

// ReloadRulesWithCandidates atomically binds managed health candidate sources
// together with their rules. Empty or failed sources have an explicit reject
// outbound, never the default handler or an observatory fallback.
func (r *Router) ReloadRulesWithCandidates(config *Config, fallbackTag string, managed map[string]ManagedBalancerCandidates) error {
	return r.reloadRules(config, false, fallbackTag, managed)
}

func (r *Router) reloadRules(config *Config, shouldAppend bool, fallbackTag string, managed map[string]ManagedBalancerCandidates) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	oldRules := *r.rules.Load()
	oldBalancers := *r.balancers.Load()

	var newRules []*Rule
	newBalancers := make(map[string]*Balancer)
	var createdWebhooks []*Rule
	committed := false
	defer func() {
		if !committed {
			closeWebhooks(createdWebhooks)
		}
	}()
	existTags := make(map[string]bool, len(oldRules)+len(config.Rule))
	if shouldAppend {
		maps.Copy(newBalancers, oldBalancers)
		for _, rule := range oldRules {
			if rule.fallback {
				fallbackTag = rule.Tag
				continue
			}
			newRules = append(newRules, rule)
			existTags[rule.RuleTag] = true
		}
	}

	for _, rule := range config.BalancingRule {
		if _, found := newBalancers[rule.Tag]; found {
			return errors.New("duplicate balancer tag")
		}
		balancer, err := rule.Build(r.ohm, r.dispatcher)
		if err != nil {
			return err
		}
		if candidates, ok := managed[rule.Tag]; ok {
			if candidates.Source == nil || candidates.RejectTag == "" || r.ohm.GetHandler(candidates.RejectTag) == nil {
				return errors.New("managed balancer requires a source and explicit reject handler")
			}
			if rule.FallbackTag != "" || (!strings.EqualFold(rule.Strategy, "random") && !strings.EqualFold(rule.Strategy, "roundrobin")) {
				return errors.New("managed candidates require an unobserved random or round-robin balancer")
			}
			balancer.candidateSource = candidates.Source
			balancer.rejectTag = candidates.RejectTag
		}
		balancer.InjectContext(r.ctx)
		newBalancers[rule.Tag] = balancer
	}
	for tag := range managed {
		if newBalancers[tag] == nil {
			return errors.New("managed candidates reference an unavailable balancer")
		}
	}

	for _, rule := range config.Rule {
		cond, err := rule.BuildCondition()
		if err != nil {
			return err
		}
		rr := &Rule{
			Condition: cond,
			Tag:       rule.GetTag(),
			RuleTag:   rule.GetRuleTag(),
		}
		if rr.RuleTag != "" && existTags[rr.RuleTag] {
			return errors.New("duplicate ruleTag ", rr.RuleTag)
		}
		existTags[rr.RuleTag] = true
		if wh := rule.GetWebhook(); wh != nil {
			notifier, err := NewWebhookNotifier(wh)
			if err != nil {
				return err
			}
			rr.Webhook = notifier
			createdWebhooks = append(createdWebhooks, rr)
		}
		if btag := rule.GetBalancingTag(); len(btag) > 0 {
			brule, found := newBalancers[btag]
			if !found {
				return errors.New("balancer ", btag, " not found")
			}
			rr.Balancer = brule
		}
		newRules = append(newRules, rr)
	}
	if fallbackTag != "" {
		newRules = append(newRules, &Rule{
			Tag: fallbackTag, Condition: NewConditionChan(), fallback: true,
		})
	}

	r.balancers.Store(&newBalancers)
	r.rules.Store(&newRules)
	committed = true
	if !shouldAppend {
		closeWebhooks(oldRules)
	}
	return nil
}

// RemoveRule implements routing.Router.
func (r *Router) RemoveRule(tag string) error {
	if tag == "" {
		return errors.New("empty tag name!")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	oldRules := *r.rules.Load()
	newRules := make([]*Rule, 0, len(oldRules))
	var removed []*Rule
	for _, rule := range oldRules {
		if rule.RuleTag != tag {
			newRules = append(newRules, rule)
		} else {
			removed = append(removed, rule)
		}
	}
	r.rules.Store(&newRules)
	closeWebhooks(removed)
	return nil
}

// ListRule implements routing.Router
func (r *Router) ListRule() []routing.Route {
	rules := *r.rules.Load()
	ruleList := make([]routing.Route, 0, len(rules))
	for _, rule := range rules {
		if rule.fallback {
			continue
		}
		ruleList = append(ruleList, &Route{
			outboundTag: rule.Tag,
			ruleTag:     rule.RuleTag,
		})
	}
	return ruleList
}

func (r *Router) pickRouteInternal(ctx routing.Context) (*Rule, routing.Context, error) {
	// SkipDNSResolve is set from DNS module.
	// the DOH remote server maybe a domain name,
	// this prevents cycle resolving dead loop
	skipDNSResolve := ctx.GetSkipDNSResolve()

	if r.domainStrategy == Config_IpOnDemand && !skipDNSResolve {
		ctx = routing_dns.ContextWithDNSClient(ctx, r.dns)
	}

	rules := *r.rules.Load()
	var fallback *Rule

	for _, rule := range rules {
		if rule.fallback {
			fallback = rule
			break
		}
		if rule.Apply(ctx) {
			return rule, ctx, nil
		}
	}

	if r.domainStrategy != Config_IpIfNonMatch || len(ctx.GetTargetDomain()) == 0 || skipDNSResolve {
		if fallback != nil {
			return fallback, ctx, nil
		}
		return nil, ctx, common.ErrNoClue
	}

	ctx = routing_dns.ContextWithDNSClient(ctx, r.dns)

	// Try applying rules again if we have IPs.
	for _, rule := range rules {
		if rule.Apply(ctx) {
			return rule, ctx, nil
		}
	}

	return nil, ctx, common.ErrNoClue
}

// Start implements common.Runnable.
func (r *Router) Start() error {
	if r.script != nil {
		if err := r.script.start(r); err != nil {
			return errors.New("failed to initialize routing script").Base(err)
		}
	}
	return nil
}

// closeWebhooks closes all webhook notifiers in the given rule set.
func closeWebhooks(rules []*Rule) {
	for _, rule := range rules {
		if rule.Webhook != nil {
			rule.Webhook.Close()
		}
	}
}

// Close implements common.Closable.
func (r *Router) Close() error {
	if r.script != nil {
		r.script.close()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	closeWebhooks(*r.rules.Load())
	return nil
}

// Type implements common.HasType.
func (*Router) Type() interface{} {
	return routing.RouterType()
}

// GetOutboundGroupTags implements routing.Route.
func (r *Route) GetOutboundGroupTags() []string {
	return r.outboundGroupTags
}

// GetOutboundTag implements routing.Route.
func (r *Route) GetOutboundTag() string {
	return r.outboundTag
}

func (r *Route) GetRuleTag() string {
	return r.ruleTag
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		r := new(Router)
		if err := core.RequireFeatures(ctx, func(d dns.Client, ohm outbound.Manager, dispatcher routing.Dispatcher) error {
			return r.Init(ctx, config.(*Config), d, ohm, dispatcher)
		}); err != nil {
			return nil, err
		}
		return r, nil
	}))
}
