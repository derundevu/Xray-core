package dns_test

import (
	"testing"

	. "github.com/xtls/xray-core/app/dns"
	featureDNS "github.com/xtls/xray-core/features/dns"
)

func TestDOHNameServer(t *testing.T) {
	endpoint, _ := loopbackDOH(t)
	server := NewDoHNameServer(endpoint, nil, false, false, false, 0, nil)
	requireLoopbackDNSQuery(t, server.QueryIP, featureDNS.IPOption{IPv4Enable: true, IPv6Enable: true})
}

func TestDOHNameServerWithCache(t *testing.T) {
	endpoint, requests := loopbackDOH(t)
	server := NewDoHNameServer(endpoint, nil, false, false, false, 0, nil)
	option := featureDNS.IPOption{IPv4Enable: true, IPv6Enable: true}
	requireLoopbackDNSQuery(t, server.QueryIP, option)
	before := requests.Load()
	requireLoopbackDNSQuery(t, server.QueryIP, option)
	if before != 2 || requests.Load() != before {
		t.Fatalf("DoH cache repeated wire queries: before=%d after=%d", before, requests.Load())
	}
}

func TestDOHNameServerWithIPv4Override(t *testing.T) {
	endpoint, _ := loopbackDOH(t)
	server := NewDoHNameServer(endpoint, nil, false, false, false, 0, nil)
	requireLoopbackDNSQuery(t, server.QueryIP, featureDNS.IPOption{IPv4Enable: true})
}

func TestDOHNameServerWithIPv6Override(t *testing.T) {
	endpoint, _ := loopbackDOH(t)
	server := NewDoHNameServer(endpoint, nil, false, false, false, 0, nil)
	requireLoopbackDNSQuery(t, server.QueryIP, featureDNS.IPOption{IPv6Enable: true})
}
