package transport

import (
	"context"
	"crypto/tls"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

type timeoutConn struct {
	net.Conn
	writeTimeout time.Duration
	readTimeout  time.Duration
}

func (c timeoutConn) Write(b []byte) (n int, err error) {
	if c.writeTimeout > 0 {
		if err := c.SetWriteDeadline(time.Now().Add(c.writeTimeout)); err != nil {
			return 0, err
		}
	}
	return c.Conn.Write(b)
}

func (c timeoutConn) Read(b []byte) (n int, err error) {
	if c.readTimeout > 0 {
		if err := c.SetReadDeadline(time.Now().Add(c.readTimeout)); err != nil {
			return 0, err
		}
	}
	return c.Conn.Read(b)
}

type rwTimeoutDialer struct {
	wtimeoutd  time.Duration
	rdtimeoutd time.Duration
	net.Dialer
}

func (d *rwTimeoutDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := d.Dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return &timeoutConn{
		readTimeout:  d.rdtimeoutd,
		writeTimeout: d.wtimeoutd,
		Conn:         conn,
	}, nil
}

// NewTimeoutTransport returns a transport created using the given TLS info.
// If read/write on the created connection blocks longer than its time limit,
// it will return timeout error.
// If read/write timeout is set, transport will not be able to reuse connection.
func NewTimeoutTransport(info TLSInfo, dialtimeoutd, rdtimeoutd, wtimeoutd time.Duration) (*http.Transport, io.Closer, error) {
	tr, h3Closer, err := NewTransport(info, dialtimeoutd)
	if err != nil {
		return nil, nil, err
	}

	if rdtimeoutd != 0 || wtimeoutd != 0 {
		// the timed out connection will timeout soon after it is idle.
		// it should not be put back to http transport as an idle connection for future usage.
		tr.MaxIdleConnsPerHost = -1
	} else {
		// allow more idle connections between peers to avoid unnecessary port allocation.
		tr.MaxIdleConnsPerHost = 1024
	}

	tr.DialContext = (&rwTimeoutDialer{
		Dialer: net.Dialer{
			Timeout:   dialtimeoutd,
			KeepAlive: 30 * time.Second,
		},
		rdtimeoutd: rdtimeoutd,
		wtimeoutd:  wtimeoutd,
	}).DialContext
	return tr, h3Closer, nil
}

type unixTransport struct{ *http.Transport }

// NewTransport creates an HTTP transport configured with the given TLS info.
// The returned io.Closer must be closed when the transport is no longer needed
// to release HTTP/3 QUIC resources (nil if HTTP/3 is not configured).
func NewTransport(info TLSInfo, dialtimeoutd time.Duration) (*http.Transport, io.Closer, error) {
	cfg, err := info.ClientConfig()
	if err != nil {
		return nil, nil, err
	}

	var ipAddr net.Addr
	if info.LocalAddr != "" {
		ipAddr, err = net.ResolveTCPAddr("tcp", info.LocalAddr+":0")
		if err != nil {
			return nil, nil, err
		}
	}

	t := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   dialtimeoutd,
			LocalAddr: ipAddr,
			// value taken from http.DefaultTransport
			KeepAlive: 30 * time.Second,
		}).DialContext,
		// value taken from http.DefaultTransport
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     cfg,
		ForceAttemptHTTP2:   true,
	}

	dialer := &net.Dialer{
		Timeout:   dialtimeoutd,
		KeepAlive: 30 * time.Second,
	}

	dialContext := func(ctx context.Context, net, addr string) (net.Conn, error) {
		return dialer.DialContext(ctx, "unix", addr)
	}
	tu := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         dialContext,
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     cfg,
		// Cost of reopening connection on sockets is low, and they are mostly used in testing.
		// Long living unix-transport connections were leading to 'leak' test flakes.
		// Alternatively the returned Transport (t) should override CloseIdleConnections to
		// forward it to 'tu' as well.
		IdleConnTimeout:   time.Microsecond,
		ForceAttemptHTTP2: true,
	}
	ut := &unixTransport{tu}

	t.RegisterProtocol("unix", ut)
	t.RegisterProtocol("unixs", ut)

	var h3Closer io.Closer
	if info.CertFile != "" {
		tr := &http3.Transport{
			TLSClientConfig: cfg,
			QUICConfig: &quic.Config{
				MaxIdleTimeout:  time.Minute,
				KeepAlivePeriod: 5 * time.Second,
			},
		}
		t.RegisterProtocol("https", tr)
		h3Closer = tr
	}

	return t, h3Closer, nil
}

func (urt *unixTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	url := *req.URL
	req.URL = &url
	req.URL.Scheme = strings.Replace(req.URL.Scheme, "unix", "http", 1)
	return urt.Transport.RoundTrip(req)
}

type TLSInfoSimple struct {
	CertFile string
	KeyFile  string
}

func (tlsInfo *TLSInfoSimple) ClientConfig() *tls.Config {
	cert, err := tls.LoadX509KeyPair(tlsInfo.CertFile, tlsInfo.KeyFile)
	if err != nil {
		log.Fatal(err)
	}

	return &tls.Config{
		// This is because we are using self-signed certificates.
		InsecureSkipVerify: true, //nolint:gosec // G402: configurable TLS for internal transport
		Certificates:       []tls.Certificate{cert},
		NextProtos:         []string{"h3"}, // Use "h3" for HTTP/3
		ClientSessionCache: tls.NewLRUClientSessionCache(100),
	}
}
func (tlsInfo *TLSInfoSimple) ServerConfig() *tls.Config {
	cert, err := tls.LoadX509KeyPair(tlsInfo.CertFile, tlsInfo.KeyFile)
	if err != nil {
		log.Fatal(err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
	}
}

// IsClosedConnError returns true if the error is from closing listener, cmux.
// copied from golang.org/x/net/http2/http2.go
func IsClosedConnError(err error) bool {
	// 'use of closed network connection' (Go <=1.8)
	// 'use of closed file or network connection' (Go >1.8, internal/poll.ErrClosing)
	// 'mux: listener closed' (cmux.ErrListenerClosed)
	return err != nil && strings.Contains(err.Error(), "closed")
}
