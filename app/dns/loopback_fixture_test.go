package dns_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apernet/quic-go"
	mdns "github.com/miekg/dns"
	featureDNS "github.com/xtls/xray-core/features/dns"
	xtls "github.com/xtls/xray-core/transport/internet/tls"
)

// httptest uses the same localhost certificate for every server. Trust only that
// public test certificate for these tests; never bypass HTTPS/QUIC verification.
func trustLoopbackDNSCertificate(t *testing.T, certificate *x509.Certificate) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "dns-fixture-ca.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SSL_CERT_FILE", path)
}

func loopbackDNSResponse(t *testing.T, payload []byte) []byte {
	t.Helper()
	var request mdns.Msg
	if err := request.Unpack(payload); err != nil {
		t.Errorf("invalid fixture DNS request: %v", err)
		return nil
	}
	response := new(mdns.Msg).SetReply(&request)
	for _, question := range request.Question {
		if question.Name != "analytics.test." {
			response.Rcode = mdns.RcodeNameError
			continue
		}
		header := mdns.RR_Header{Name: question.Name, Rrtype: question.Qtype, Class: mdns.ClassINET, Ttl: 60}
		switch question.Qtype {
		case mdns.TypeA:
			response.Answer = append(response.Answer, &mdns.A{Hdr: header, A: net.ParseIP("192.0.2.42")})
		case mdns.TypeAAAA:
			response.Answer = append(response.Answer, &mdns.AAAA{Hdr: header, AAAA: net.ParseIP("2001:db8::42")})
		}
	}
	encoded, err := response.Pack()
	if err != nil {
		t.Errorf("encode fixture DNS response: %v", err)
		return nil
	}
	return encoded
}

func loopbackDOH(t *testing.T) (*url.URL, *atomic.Int64) {
	t.Helper()
	requests := new(atomic.Int64)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/dns-query" || r.ProtoMajor != 2 {
			http.Error(w, "unexpected DoH request", http.StatusBadRequest)
			return
		}
		payload, err := io.ReadAll(io.LimitReader(r.Body, 65536))
		if err != nil {
			t.Errorf("read fixture DoH request: %v", err)
			return
		}
		requests.Add(1)
		w.Header().Set("Content-Type", "application/dns-message")
		w.Write(loopbackDNSResponse(t, payload))
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)
	trustLoopbackDNSCertificate(t, server.Certificate())
	endpoint, err := url.Parse(server.URL + "/dns-query")
	if err != nil {
		t.Fatal(err)
	}
	endpoint.Scheme = "https+local"
	return endpoint, requests
}

func loopbackTCPDNS(t *testing.T) (*url.URL, *atomic.Int64) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	requests := new(atomic.Int64)
	ready := make(chan struct{})
	server := &mdns.Server{Listener: listener, Net: "tcp", NotifyStartedFunc: func() { close(ready) }, Handler: mdns.HandlerFunc(func(w mdns.ResponseWriter, r *mdns.Msg) {
		requests.Add(1)
		payload, err := r.Pack()
		if err != nil {
			t.Errorf("pack TCP DNS request: %v", err)
			return
		}
		var response mdns.Msg
		if err := response.Unpack(loopbackDNSResponse(t, payload)); err != nil {
			t.Errorf("unpack TCP DNS response: %v", err)
			return
		}
		if err := w.WriteMsg(&response); err != nil {
			t.Errorf("write TCP DNS response: %v", err)
		}
	})}
	done := make(chan error, 1)
	go func() { done <- server.ActivateAndServe() }()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("TCP DNS fixture did not start")
	}
	t.Cleanup(func() {
		server.Shutdown()
		if err := <-done; err != nil {
			t.Errorf("TCP DNS server failed: %v", err)
		}
	})
	endpoint, err := url.Parse("tcp+local://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return endpoint, requests
}

func loopbackQUICDNS(t *testing.T) (*url.URL, *atomic.Int64, *xtls.Config) {
	t.Helper()
	certificateServer := httptest.NewTLSServer(http.NotFoundHandler())
	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateServer.Certificate().Raw})
	configuration := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: certificateServer.TLS.Certificates, NextProtos: []string{"doq"}}
	certificateServer.Close()
	listener, err := quic.ListenAddr("127.0.0.1:0", configuration, &quic.Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	requests := new(atomic.Int64)
	var mu sync.Mutex
	connections := make(map[*quic.Conn]struct{})
	var workers sync.WaitGroup
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for {
			connection, err := listener.Accept(ctx)
			if err != nil {
				return
			}
			mu.Lock()
			connections[connection] = struct{}{}
			mu.Unlock()
			workers.Go(func() {
				defer func() { mu.Lock(); delete(connections, connection); mu.Unlock() }()
				defer connection.CloseWithError(0, "")
				for {
					stream, err := connection.AcceptStream(ctx)
					if err != nil {
						return
					}
					stream.SetDeadline(time.Now().Add(5 * time.Second))
					var prefix [2]byte
					if _, err := io.ReadFull(stream, prefix[:]); err != nil {
						return
					}
					payload := make([]byte, int(binary.BigEndian.Uint16(prefix[:])))
					if _, err := io.ReadFull(stream, payload); err != nil {
						return
					}
					requests.Add(1)
					response := loopbackDNSResponse(t, payload)
					binary.BigEndian.PutUint16(prefix[:], uint16(len(response)))
					if _, err := stream.Write(append(prefix[:], response...)); err != nil {
						return
					}
					stream.Close()
				}
			})
		}
	}()
	t.Cleanup(func() {
		cancel()
		listener.Close()
		<-accepted
		mu.Lock()
		for connection := range connections {
			connection.CloseWithError(0, "")
		}
		mu.Unlock()
		workers.Wait()
	})
	endpoint, err := url.Parse("quic://" + listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return endpoint, requests, &xtls.Config{
		DisableSystemRoot: true,
		Certificate:       []*xtls.Certificate{{Certificate: certificate, Usage: xtls.Certificate_AUTHORITY_VERIFY}},
	}
}

func requireLoopbackDNSQuery(t *testing.T, query func(context.Context, string, featureDNS.IPOption) ([]net.IP, uint32, error), option featureDNS.IPOption) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ips, _, err := query(ctx, "analytics.test", option)
	if err != nil {
		t.Fatal(err)
	}
	var got, want []string
	for _, ip := range ips {
		got = append(got, ip.String())
	}
	if option.IPv4Enable {
		want = append(want, "192.0.2.42")
	}
	if option.IPv6Enable {
		want = append(want, "2001:db8::42")
	}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DNS family/response mismatch: got %v want %v", got, want)
	}
}
