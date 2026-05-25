// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transport

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"slices"

	"go.uber.org/zap"
)

type TLSInfo struct {
	// CertFile is the _server_ cert, it will also be used as a _client_ certificate if ClientCertFile is empty
	CertFile string
	// KeyFile is the key for the CertFile
	KeyFile string
	// ClientCertFile is a _client_ cert for initiating connections when ClientCertAuth is defined. If ClientCertAuth
	// is true but this value is empty, the CertFile will be used instead.
	ClientCertFile string
	// ClientKeyFile is the key for the ClientCertFile
	ClientKeyFile string

	TrustedCAFile       string
	ClientCertAuth      bool
	CRLFile             string
	InsecureSkipVerify  bool
	SkipClientSANVerify bool

	// ServerName ensures the cert matches the given host in case of discovery / virtual hosting
	ServerName string

	// HandshakeFailure is optionally called when a connection fails to handshake. The
	// connection will be closed immediately afterwards.
	HandshakeFailure func(*tls.Conn, error)

	// CipherSuites is a list of supported cipher suites.
	// If empty, Go auto-populates it by default.
	// Note that cipher suites are prioritized in the given order.
	CipherSuites []uint16

	// MinVersion is the minimum TLS version that is acceptable.
	// If not set, the minimum version is TLS 1.2.
	MinVersion uint16

	// MaxVersion is the maximum TLS version that is acceptable.
	// If not set, the default used by Go is selected (see tls.Config.MaxVersion).
	MaxVersion uint16

	selfCert bool

	// parseFunc exists to simplify testing. Typically, parseFunc
	// should be left nil. In that case, tls.X509KeyPair will be used.
	parseFunc func([]byte, []byte) (tls.Certificate, error)

	// AllowedCNs is a list of acceptable CNs which must be provided by a client.
	AllowedCNs []string

	// AllowedHostnames is a list of acceptable IP addresses or hostnames that must match the
	// TLS certificate provided by a client.
	AllowedHostnames []string

	// Logger logs TLS errors.
	// If nil, all logs are discarded.
	Logger *zap.Logger

	// EmptyCN indicates that the cert must have empty CN.
	// If true, ClientConfig() will return an error for a cert with non empty CN.
	EmptyCN bool

	// LocalAddr is the local IP address to use when communicating with a peer.
	LocalAddr string
}

func (info TLSInfo) String() string {
	return fmt.Sprintf("cert = %s, key = %s, client-cert=%s, client-key=%s, trusted-ca = %s, client-cert-auth = %v, crl-file = %s", info.CertFile, info.KeyFile, info.ClientCertFile, info.ClientKeyFile, info.TrustedCAFile, info.ClientCertAuth, info.CRLFile)
}

func (info TLSInfo) Empty() bool {
	return info.CertFile == "" && info.KeyFile == ""
}

// NewCert generates TLS cert by using the given cert,key and parse function.
func NewCert(certfile, keyfile string, parseFunc func([]byte, []byte) (tls.Certificate, error)) (*tls.Certificate, error) {
	cert, err := os.ReadFile(certfile) //nolint:gosec // G304: internal file I/O, not user-controlled
	if err != nil {
		return nil, err
	}

	key, err := os.ReadFile(keyfile) //nolint:gosec // G304: internal file I/O, not user-controlled
	if err != nil {
		return nil, err
	}

	if parseFunc == nil {
		parseFunc = tls.X509KeyPair
	}

	tlsCert, err := parseFunc(cert, key)
	if err != nil {
		return nil, err
	}
	return &tlsCert, nil
}

// NewCertPool creates x509 certPool with provided CA files.
func NewCertPool(CAFiles []string) (*x509.CertPool, error) {
	certPool := x509.NewCertPool()

	for _, CAFile := range CAFiles {
		pemByte, err := os.ReadFile(CAFile) //nolint:gosec // G304: internal file I/O, not user-controlled
		if err != nil {
			return nil, err
		}

		for {
			var block *pem.Block
			block, pemByte = pem.Decode(pemByte)
			if block == nil {
				break
			}
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return nil, err
			}

			certPool.AddCert(cert)
		}
	}

	return certPool, nil
}

// baseConfig is called on initial TLS handshake start.
//
// Previously,
// 1. Server has non-empty (*tls.Config).Certificates on client hello
// 2. Server calls (*tls.Config).GetCertificate iff:
//   - Server's (*tls.Config).Certificates is not empty, or
//   - Client supplies SNI; non-empty (*tls.ClientHelloInfo).ServerName
//
// When (*tls.Config).Certificates is always populated on initial handshake,
// client is expected to provide a valid matching SNI to pass the TLS
// verification, thus trigger server (*tls.Config).GetCertificate to reload
// TLS assets. However, a cert whose SAN field does not include domain names
// but only IP addresses, has empty (*tls.ClientHelloInfo).ServerName, thus
// it was never able to trigger TLS reload on initial handshake; first
// ceritifcate object was being used, never being updated.
//
// Now, (*tls.Config).Certificates is created empty on initial TLS client
// handshake, in order to trigger (*tls.Config).GetCertificate and populate
// rest of the certificates on every new TLS connection, even when client
// SNI is empty (e.g. cert only includes IPs).
func (info TLSInfo) baseConfig() (*tls.Config, error) {
	if info.KeyFile == "" || info.CertFile == "" {
		return nil, fmt.Errorf("KeyFile and CertFile must both be present[key: %v, cert: %v]", info.KeyFile, info.CertFile)
	}
	if info.Logger == nil {
		info.Logger = zap.NewNop()
	}

	_, err := NewCert(info.CertFile, info.KeyFile, info.parseFunc)
	if err != nil {
		return nil, err
	}

	// Perform prevalidation of client cert and key if either are provided. This makes sure we crash before accepting any connections.
	if (info.ClientKeyFile == "") != (info.ClientCertFile == "") {
		return nil, fmt.Errorf("ClientKeyFile and ClientCertFile must both be present or both absent: key: %v, cert: %v]", info.ClientKeyFile, info.ClientCertFile)
	}
	if info.ClientCertFile != "" {
		_, err := NewCert(info.ClientCertFile, info.ClientKeyFile, info.parseFunc)
		if err != nil {
			return nil, err
		}
	}

	var minVersion uint16
	if info.MinVersion != 0 {
		minVersion = info.MinVersion
	} else {
		// Default minimum version is TLS 1.2, previous versions are insecure and deprecated.
		minVersion = tls.VersionTLS13
	}

	cfg := &tls.Config{ //nolint:gosec // G402: configurable TLS for internal transport
		MinVersion: minVersion,
		MaxVersion: info.MaxVersion,
		ServerName: info.ServerName,
	}

	if len(info.CipherSuites) > 0 {
		cfg.CipherSuites = info.CipherSuites
	}

	// Client certificates may be verified by either an exact match on the CN,
	// or a more general check of the CN and SANs.
	var verifyCertificate func(*x509.Certificate) bool

	if len(info.AllowedCNs) > 0 && len(info.AllowedHostnames) > 0 {
		return nil, fmt.Errorf("AllowedCNs and AllowedHostnames are mutually exclusive (cns=%q, hostnames=%q)", info.AllowedCNs, info.AllowedHostnames)
	}

	if len(info.AllowedCNs) > 0 {
		verifyCertificate = func(cert *x509.Certificate) bool {
			return slices.Contains(info.AllowedCNs, cert.Subject.CommonName)
		}
	}
	if len(info.AllowedHostnames) > 0 {
		verifyCertificate = func(cert *x509.Certificate) bool {
			for _, allowedHostname := range info.AllowedHostnames {
				if cert.VerifyHostname(allowedHostname) == nil {
					return true
				}
			}
			return false
		}
	}
	if verifyCertificate != nil {
		cfg.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error { //nolint:gosec // G123: intentional custom certificate verification for mTLS
			for _, chains := range verifiedChains {
				if len(chains) != 0 {
					if verifyCertificate(chains[0]) {
						return nil
					}
				}
			}
			return errors.New("client certificate authentication failed")
		}
	}

	// this only reloads certs when there's a client request
	// TODO: support server-side refresh (e.g. inotify, SIGHUP), caching
	cfg.GetCertificate = func(clientHello *tls.ClientHelloInfo) (cert *tls.Certificate, err error) {
		cert, err = NewCert(info.CertFile, info.KeyFile, info.parseFunc)
		if os.IsNotExist(err) {
			info.Logger.Warn(
				"failed to find peer cert files",
				zap.String("cert-file", info.CertFile),
				zap.String("key-file", info.KeyFile),
				zap.Error(err),
			)
		} else if err != nil {
			info.Logger.Warn(
				"failed to create peer certificate",
				zap.String("cert-file", info.CertFile),
				zap.String("key-file", info.KeyFile),
				zap.Error(err),
			)
		}
		return cert, err
	}
	cfg.GetClientCertificate = func(unused *tls.CertificateRequestInfo) (cert *tls.Certificate, err error) {
		certfile, keyfile := info.CertFile, info.KeyFile
		if info.ClientCertFile != "" {
			certfile, keyfile = info.ClientCertFile, info.ClientKeyFile
		}
		cert, err = NewCert(certfile, keyfile, info.parseFunc)
		if os.IsNotExist(err) {
			info.Logger.Warn(
				"failed to find client cert files",
				zap.String("cert-file", certfile),
				zap.String("key-file", keyfile),
				zap.Error(err),
			)
		} else if err != nil {
			info.Logger.Warn(
				"failed to create client certificate",
				zap.String("cert-file", certfile),
				zap.String("key-file", keyfile),
				zap.Error(err),
			)
		}
		return cert, err
	}
	return cfg, nil
}

// cafiles returns a list of CA file paths.
func (info TLSInfo) cafiles() []string {
	cs := make([]string, 0)
	if info.TrustedCAFile != "" {
		cs = append(cs, info.TrustedCAFile)
	}
	return cs
}

// ServerConfig generates a tls.Config object for use by an HTTP server.
func (info TLSInfo) ServerConfig() (*tls.Config, error) {
	cfg, err := info.baseConfig()
	if err != nil {
		return nil, err
	}

	if info.Logger == nil {
		info.Logger = zap.NewNop()
	}

	cfg.ClientAuth = tls.NoClientCert
	if info.TrustedCAFile != "" || info.ClientCertAuth {
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	cs := info.cafiles()
	if len(cs) > 0 {
		info.Logger.Info("Loading cert pool", zap.Strings("cs", cs),
			zap.Any("tlsinfo", info))
		cp, err := NewCertPool(cs)
		if err != nil {
			return nil, err
		}
		cfg.ClientCAs = cp
	}

	// "h3" NextProtos is necessary for enabling HTTP3 for quic-go
	// "h2" NextProtos is necessary for enabling HTTP2 for go's HTTP server
	cfg.NextProtos = []string{"h3", "h2"}
	cfg.ClientSessionCache = tls.NewLRUClientSessionCache(100)

	return cfg, nil
}

// ClientConfig generates a tls.Config object for use by an HTTP client.
func (info TLSInfo) ClientConfig() (*tls.Config, error) {
	var cfg *tls.Config
	var err error

	if !info.Empty() {
		cfg, err = info.baseConfig()
		if err != nil {
			return nil, err
		}
	} else {
		cfg = &tls.Config{ServerName: info.ServerName} //nolint:gosec // G402: configurable TLS for internal transport
	}
	cfg.InsecureSkipVerify = info.InsecureSkipVerify

	cs := info.cafiles()
	if len(cs) > 0 {
		cfg.RootCAs, err = NewCertPool(cs)
		if err != nil {
			return nil, err
		}
	}

	if info.selfCert {
		cfg.InsecureSkipVerify = true
	}

	if info.EmptyCN {
		hasNonEmptyCN := false
		cn := ""
		_, err := NewCert(info.CertFile, info.KeyFile, func(certPEMBlock []byte, keyPEMBlock []byte) (tls.Certificate, error) {
			var block *pem.Block
			block, _ = pem.Decode(certPEMBlock)
			cert, err := x509.ParseCertificate(block.Bytes)
			if err != nil {
				return tls.Certificate{}, err
			}
			if len(cert.Subject.CommonName) != 0 {
				hasNonEmptyCN = true
				cn = cert.Subject.CommonName
			}
			return tls.X509KeyPair(certPEMBlock, keyPEMBlock)
		})
		if err != nil {
			return nil, err
		}
		if hasNonEmptyCN {
			return nil, fmt.Errorf("cert has non empty Common Name (%s): %s", cn, info.CertFile)
		}
	}

	return cfg, nil
}
