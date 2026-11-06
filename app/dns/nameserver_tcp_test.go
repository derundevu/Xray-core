package dns_test

import (
	"testing"

	. "github.com/xtls/xray-core/app/dns"
	featureDNS "github.com/xtls/xray-core/features/dns"
)

func TestTCPLocalNameServer(t *testing.T) {
	endpoint, _ := loopbackTCPDNS(t)
	server, err := NewTCPLocalNameServer(endpoint, false, false, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	requireLoopbackDNSQuery(t, server.QueryIP, featureDNS.IPOption{IPv4Enable: true, IPv6Enable: true})
}

func TestTCPLocalNameServerWithCache(t *testing.T) {
	endpoint, requests := loopbackTCPDNS(t)
	server, err := NewTCPLocalNameServer(endpoint, false, false, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	option := featureDNS.IPOption{IPv4Enable: true, IPv6Enable: true}
	requireLoopbackDNSQuery(t, server.QueryIP, option)
	before := requests.Load()
	requireLoopbackDNSQuery(t, server.QueryIP, option)
	if before != 2 || requests.Load() != before {
		t.Fatalf("TCP DNS cache repeated wire queries: before=%d after=%d", before, requests.Load())
	}
}

func TestTCPLocalNameServerWithIPv4Override(t *testing.T) {
	endpoint, _ := loopbackTCPDNS(t)
	server, err := NewTCPLocalNameServer(endpoint, false, false, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	requireLoopbackDNSQuery(t, server.QueryIP, featureDNS.IPOption{IPv4Enable: true})
}

func TestTCPLocalNameServerWithIPv6Override(t *testing.T) {
	endpoint, _ := loopbackTCPDNS(t)
	server, err := NewTCPLocalNameServer(endpoint, false, false, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	requireLoopbackDNSQuery(t, server.QueryIP, featureDNS.IPOption{IPv6Enable: true})
}
