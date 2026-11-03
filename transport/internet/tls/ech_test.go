package tls

import (
	"context"
	"crypto/ecdh"
	"crypto/rand"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miekg/dns"
	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"golang.org/x/crypto/cryptobyte"
)

func localECHKeys(t *testing.T) (configList, serverKeys []byte) {
	t.Helper()
	private, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	var config cryptobyte.Builder
	config.AddUint16(0xfe0d)
	config.AddUint16LengthPrefixed(func(contents *cryptobyte.Builder) {
		contents.AddUint8(0)
		contents.AddUint16(0x0020) // DHKEM(X25519, HKDF-SHA256).
		contents.AddUint16LengthPrefixed(func(public *cryptobyte.Builder) {
			public.AddBytes(private.PublicKey().Bytes())
		})
		contents.AddUint16LengthPrefixed(func(suites *cryptobyte.Builder) {
			suites.AddUint16(1) // HKDF-SHA256.
			suites.AddUint16(1) // AES-128-GCM.
		})
		contents.AddUint8(0)
		contents.AddUint8LengthPrefixed(func(name *cryptobyte.Builder) { name.AddBytes([]byte("public.test")) })
		contents.AddUint16(0)
	})
	configBytes, err := config.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	var list, keys cryptobyte.Builder
	list.AddUint16LengthPrefixed(func(value *cryptobyte.Builder) { value.AddBytes(configBytes) })
	keys.AddUint16LengthPrefixed(func(value *cryptobyte.Builder) { value.AddBytes(private.Bytes()) })
	keys.AddUint16LengthPrefixed(func(value *cryptobyte.Builder) { value.AddBytes(configBytes) })
	configList, err = list.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	serverKeys, err = keys.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	return
}

func localECHDNS(t *testing.T, configList []byte, rcode int) (string, *atomic.Int64) {
	t.Helper()
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int64
	started := make(chan struct{})
	server := &dns.Server{PacketConn: packet, NotifyStartedFunc: func() { close(started) }}
	server.Handler = dns.HandlerFunc(func(w dns.ResponseWriter, request *dns.Msg) {
		requests.Add(1)
		response := new(dns.Msg)
		response.SetReply(request)
		response.Rcode = rcode
		if rcode == dns.RcodeSuccess && len(request.Question) == 1 && request.Question[0].Qtype == dns.TypeHTTPS {
			response.Answer = []dns.RR{&dns.HTTPS{SVCB: dns.SVCB{
				Hdr:      dns.RR_Header{Name: request.Question[0].Name, Rrtype: dns.TypeHTTPS, Class: dns.ClassINET, Ttl: 300},
				Priority: 1, Target: ".", Value: []dns.SVCBKeyValue{&dns.SVCBECHConfig{ECH: configList}},
			}}}
		}
		if err := w.WriteMsg(response); err != nil {
			t.Errorf("local DNS reply: %v", err)
		}
	})
	finished := make(chan error, 1)
	go func() { finished <- server.ActivateAndServe() }()
	t.Cleanup(func() {
		server.Shutdown()
		if err := <-finished; err != nil {
			t.Errorf("local DNS server: %v", err)
		}
	})
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("local DNS server did not start")
	}
	address := "udp://" + packet.LocalAddr().String()
	t.Cleanup(func() { GlobalECHConfigCache.Delete(ECHCacheKey(address, "secret.test", nil)) })
	return address, &requests
}

func localECHHTTP(t *testing.T, serverKeys []byte, requireECH bool) (*httptest.Server, *x509.CertPool, *atomic.Int64) {
	t.Helper()
	certificate, _ := cert.MustGenerate(nil, cert.CommonName("secret.test"), cert.DNSNames("secret.test", "public.test"))
	entry := ParseCertificate(certificate)
	entry.OneTimeLoading = true
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(entry.Certificate) {
		t.Fatal("failed to trust local certificate")
	}
	var requests atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		if requireECH && !request.TLS.ECHAccepted {
			http.Error(w, "ECH was not accepted", http.StatusForbidden)
			return
		}
		io.WriteString(w, "local ECH recipient")
	}))
	server.TLS = (&Config{Certificate: []*Certificate{entry}, EchServerKeys: serverKeys}).GetTLSConfig()
	server.StartTLS()
	t.Cleanup(server.Close)
	return server, pool, &requests
}

func localECHClient(config *Config, pool *x509.CertPool, address string) (*http.Client, *http.Transport) {
	clientTLS := config.GetTLSConfig()
	clientTLS.RootCAs = pool
	clientTLS.NextProtos = []string{"http/1.1"}
	transport := &http.Transport{
		TLSClientConfig: clientTLS,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", address)
		},
	}
	return &http.Client{Transport: transport, Timeout: 5 * time.Second}, transport
}

func TestECHDial(t *testing.T) {
	configList, serverKeys := localECHKeys(t)
	dnsAddress, queries := localECHDNS(t, configList, dns.RcodeSuccess)
	server, pool, recipients := localECHHTTP(t, serverKeys, true)
	config := &Config{ServerName: "secret.test", EchConfigList: dnsAddress}
	var clients sync.WaitGroup
	failures := make(chan error, 10)
	for range 10 {
		clients.Go(func() {
			client, transport := localECHClient(config, pool, server.Listener.Addr().String())
			defer transport.CloseIdleConnections()
			response, err := client.Get("https://secret.test/")
			if err != nil {
				failures <- err
				return
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil || response.StatusCode != http.StatusOK || string(body) != "local ECH recipient" || !response.TLS.ECHAccepted {
				failures <- fmt.Errorf("ECH request failed: status=%d, body=%q, read=%v, accepted=%t", response.StatusCode, body, err, response.TLS.ECHAccepted)
			}
		})
	}
	clients.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
	if got := queries.Load(); got != 1 {
		t.Errorf("concurrent clients made %d DNS queries; expected one cached lookup", got)
	}
	if got := recipients.Load(); got != 10 {
		t.Errorf("ECH recipient served %d requests; expected 10", got)
	}
}

func TestECHDialFail(t *testing.T) {
	for _, test := range []struct {
		name   string
		rcode  int
		config []byte
	}{
		{name: "missing DNS record", rcode: dns.RcodeNameError},
		{name: "malformed ECH record", rcode: dns.RcodeSuccess, config: []byte{0, 1, 2}},
	} {
		t.Run(test.name, func(t *testing.T) {
			dnsAddress, _ := localECHDNS(t, test.config, test.rcode)
			server, pool, recipients := localECHHTTP(t, nil, false)
			client, transport := localECHClient(&Config{ServerName: "secret.test", EchConfigList: dnsAddress}, pool, server.Listener.Addr().String())
			defer transport.CloseIdleConnections()
			if response, err := client.Get("https://secret.test/"); err == nil {
				response.Body.Close()
				t.Fatal("unavailable ECH configuration allowed plaintext SNI fallback")
			}
			if recipients.Load() != 0 {
				t.Fatal("HTTP recipient received traffic after ECH setup failed")
			}
			control, controlTransport := localECHClient(&Config{ServerName: "secret.test"}, pool, server.Listener.Addr().String())
			defer controlTransport.CloseIdleConnections()
			response, err := control.Get("https://secret.test/")
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil || response.StatusCode != http.StatusOK || string(body) != "local ECH recipient" || recipients.Load() != 1 {
				t.Fatal("local non-ECH control did not reach the same recipient")
			}
		})
	}
}
