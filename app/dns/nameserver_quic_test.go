package dns_test

import (
	"testing"

	. "github.com/xtls/xray-core/app/dns"
	featureDNS "github.com/xtls/xray-core/features/dns"
)

func TestQUICNameServer(t *testing.T) {
	endpoint, requests, security := loopbackQUICDNS(t)
	server, err := NewQUICNameServer(endpoint, false, false, 0, nil, security)
	if err != nil {
		t.Fatal(err)
	}
	option := featureDNS.IPOption{IPv4Enable: true, IPv6Enable: true}
	requireLoopbackDNSQuery(t, server.QueryIP, option)
	before := requests.Load()
	requireLoopbackDNSQuery(t, server.QueryIP, option)
	if before != 2 || requests.Load() != before {
		t.Fatalf("QUIC DNS cache repeated wire queries: before=%d after=%d", before, requests.Load())
	}
}

func TestQUICNameServerWithIPv4Override(t *testing.T) {
	endpoint, _, security := loopbackQUICDNS(t)
	server, err := NewQUICNameServer(endpoint, false, false, 0, nil, security)
	if err != nil {
		t.Fatal(err)
	}
	requireLoopbackDNSQuery(t, server.QueryIP, featureDNS.IPOption{IPv4Enable: true})
}

func TestQUICNameServerWithIPv6Override(t *testing.T) {
	endpoint, _, security := loopbackQUICDNS(t)
	server, err := NewQUICNameServer(endpoint, false, false, 0, nil, security)
	if err != nil {
		t.Fatal(err)
	}
	requireLoopbackDNSQuery(t, server.QueryIP, featureDNS.IPOption{IPv6Enable: true})
}
