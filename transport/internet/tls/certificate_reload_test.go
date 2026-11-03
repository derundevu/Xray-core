package tls

import (
	"bytes"
	"crypto"
	gotls "crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/protocol/tls/cert"
	"golang.org/x/crypto/ocsp"
)

func TestCertificateRefreshKeepsPublishedSnapshotsImmutable(t *testing.T) {
	type reply struct {
		serial string
		staple []byte
	}
	var response atomic.Pointer[reply]
	responder := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read request", http.StatusBadRequest)
			return
		}
		request, err := ocsp.ParseRequest(body)
		current := response.Load()
		if err != nil || current == nil || request.SerialNumber.String() != current.serial {
			http.Error(w, "unexpected OCSP certificate", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/ocsp-response")
		w.Write(current.staple)
	}))
	defer responder.Close()

	ca, _ := cert.MustGenerate(nil, cert.Authority(true), cert.KeyUsage(x509.KeyUsageCertSign))
	issuer, err := x509.ParseCertificate(ca.Certificate)
	if err != nil {
		t.Fatal(err)
	}
	privateKey, err := x509.ParsePKCS8PrivateKey(ca.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	makeEntry := func() *Certificate {
		leaf, _ := cert.MustGenerate(ca, cert.CommonName("reload.invalid"), cert.DNSNames("reload.invalid"), func(c *x509.Certificate) {
			c.OCSPServer = []string{responder.URL}
		})
		entry := ParseCertificate(leaf)
		caPEM, _ := ca.ToPEM()
		entry.Certificate = append(entry.Certificate, caPEM...)
		entry.OneTimeLoading = true
		return entry
	}
	first, second := makeEntry(), makeEntry()
	makeReply := func(entry *Certificate, nextUpdate time.Time) *reply {
		pair := loadX509KeyPair(entry)
		if pair == nil {
			t.Fatal("fixture key pair is invalid")
		}
		staple, err := ocsp.CreateResponse(issuer, issuer, ocsp.Response{
			Status: ocsp.Good, SerialNumber: pair.Leaf.SerialNumber,
			ThisUpdate: time.Now().Add(-time.Minute), NextUpdate: nextUpdate,
		}, privateKey.(crypto.Signer))
		if err != nil {
			t.Fatal(err)
		}
		return &reply{serial: pair.Leaf.SerialNumber.String(), staple: staple}
	}
	firstReply := makeReply(first, time.Now().Add(time.Hour))
	updatedReply := makeReply(first, time.Now().Add(2*time.Hour))
	secondReply := makeReply(second, time.Now().Add(time.Hour))

	slots := (&Config{Certificate: []*Certificate{first}}).BuildCertificates()
	getCertificate := getNewGetCertificateFunc(slots, true)
	hello := &gotls.ClientHelloInfo{ServerName: "reload.invalid"}
	selected := func() *gotls.Certificate {
		pair, err := getCertificate(hello)
		if err != nil {
			t.Fatal(err)
		}
		return pair
	}
	initial := selected()
	response.Store(firstReply)
	refreshCertificate(&slots[0], first, false, true)
	withStaple := selected()
	if !bytes.Equal(withStaple.OCSPStaple, firstReply.staple) || len(initial.OCSPStaple) != 0 {
		t.Fatal("first OCSP refresh changed an already published certificate")
	}
	response.Store(updatedReply)
	refreshCertificate(&slots[0], first, false, true)
	updated := selected()
	if !bytes.Equal(updated.OCSPStaple, updatedReply.staple) || !bytes.Equal(withStaple.OCSPStaple, firstReply.staple) {
		t.Fatal("OCSP refresh mutated the previous handshake snapshot")
	}
	invalid := &Certificate{Certificate: first.Certificate, Key: second.Key}
	refreshCertificate(&slots[0], invalid, true, false)
	if selected() != updated {
		t.Fatal("invalid replacement discarded the last valid certificate")
	}

	stop := make(chan struct{})
	failures := make(chan error, 4)
	var readers sync.WaitGroup
	for range 4 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				pair, err := getCertificate(hello)
				if err == nil {
					var staple *ocsp.Response
					staple, err = ocsp.ParseResponse(pair.OCSPStaple, issuer)
					if err == nil && staple.SerialNumber.Cmp(pair.Leaf.SerialNumber) != 0 {
						err = fmt.Errorf("certificate and OCSP response belong to different revisions")
					}
				}
				if err != nil {
					failures <- err
					return
				}
			}
		}()
	}
	var stopOnce sync.Once
	stopReaders := func() {
		stopOnce.Do(func() { close(stop) })
		readers.Wait()
	}
	defer stopReaders()
	for i := range 20 {
		entry, staple := second, secondReply
		if i%2 != 0 {
			entry, staple = first, firstReply
		}
		response.Store(staple)
		refreshCertificate(&slots[0], entry, true, true)
	}
	stopReaders()
	select {
	case err := <-failures:
		t.Fatal(err)
	default:
	}
	response.Store(secondReply)
	refreshCertificate(&slots[0], second, true, true)
	final := selected()
	if final.Leaf.SerialNumber.String() != secondReply.serial || !bytes.Equal(final.OCSPStaple, secondReply.staple) {
		t.Fatal("replacement certificate/OCSP revision was not selected")
	}
	if initial.Leaf.SerialNumber.String() != firstReply.serial || len(initial.OCSPStaple) != 0 || !bytes.Equal(withStaple.OCSPStaple, firstReply.staple) {
		t.Fatal("later reload changed a retained handshake certificate")
	}
	if _, err := getCertificate(&gotls.ClientHelloInfo{ServerName: "unknown.invalid"}); err == nil {
		t.Fatal("reload bypassed unknown-SNI rejection")
	}
}

func TestCertificateSlotsRetainIndependentSNISelection(t *testing.T) {
	makeEntry := func(name string) *Certificate {
		certificate, _ := cert.MustGenerate(nil, cert.CommonName(name), cert.DNSNames(name))
		entry := ParseCertificate(certificate)
		entry.OneTimeLoading = true
		return entry
	}
	first, second := makeEntry("first.invalid"), makeEntry("second.invalid")
	authority := makeEntry("authority.invalid")
	authority.Usage = Certificate_AUTHORITY_ISSUE
	slots := (&Config{Certificate: []*Certificate{first, {Certificate: []byte("invalid")}, authority, second}}).BuildCertificates()
	selectCertificate := getNewGetCertificateFunc(slots, true)
	requireName := func(name string) {
		t.Helper()
		pair, err := selectCertificate(&gotls.ClientHelloInfo{ServerName: name})
		if err != nil || pair.Leaf.Subject.CommonName != name {
			t.Fatalf("SNI %s selected the wrong certificate: %v", name, err)
		}
	}
	requireName("first.invalid")
	requireName("second.invalid")
	replacement := makeEntry("replacement.invalid")
	refreshCertificate(&slots[0], replacement, true, false)
	requireName("replacement.invalid")
	requireName("second.invalid")
	for _, rejected := range []string{"first.invalid", "authority.invalid"} {
		if _, err := selectCertificate(&gotls.ClientHelloInfo{ServerName: rejected}); err == nil {
			t.Fatalf("SNI %s unexpectedly selected a certificate", rejected)
		}
	}
}
