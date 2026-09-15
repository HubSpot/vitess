/*
Copyright 2019 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vttls

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"strings"
	"sync"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// SslMode indicates the type of SSL mode to use. This matches
// the MySQL SSL modes as mentioned at:
// https://dev.mysql.com/doc/refman/8.0/en/connection-options.html#option_general_ssl-mode
type SslMode string

// Disabled disables SSL and connects over plain text
const Disabled SslMode = "disabled"

// Preferred establishes an SSL connection if the server supports it.
// It does not validate the certificate provided by the server.
const Preferred SslMode = "preferred"

// Required requires an SSL connection to the server.
// It does not validate the certificate provided by the server.
const Required SslMode = "required"

// VerifyCA requires an SSL connection to the server.
// It validates the CA against the configured CA certificate(s).
const VerifyCA SslMode = "verify_ca"

// VerifyIdentity requires an SSL connection to the server.
// It validates the CA against the configured CA certificate(s) and
// also validates the certificate based on the hostname.
// This is the setting you want when you want to connect safely to
// a MySQL server and want to be protected against man-in-the-middle
// attacks.
const VerifyIdentity SslMode = "verify_identity"

// String returns the string representation, part of the Value interface
// for allowing this to be retrieved for a flag.
func (mode *SslMode) String() string {
	return string(*mode)
}

// Type returns the value type, part of the pflag Value interface
// for allowing this to be used as a generic flag.
func (mode *SslMode) Type() string {
	return "SslMode"
}

// Set updates the value of the SslMode pointer, part of the Value interface
// for allowing to update a flag.
func (mode *SslMode) Set(value string) error {
	parsedMode := SslMode(strings.ToLower(value))
	switch parsedMode {
	case "":
		*mode = Preferred
		return nil
	case Disabled, Preferred, Required, VerifyCA, VerifyIdentity:
		*mode = parsedMode
		return nil
	}
	return vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "Invalid SSL mode specified: %s. Allowed options are disabled, preferred, required, verify_ca, verify_identity", value)
}

// TLSVersionToNumber converts a text description of the TLS protocol
// to the internal Go number representation.
func TLSVersionToNumber(tlsVersion string) (uint16, error) {
	switch strings.ToLower(tlsVersion) {
	case "tlsv1.3":
		return tls.VersionTLS13, nil
	case "", "tlsv1.2":
		return tls.VersionTLS12, nil
	case "tlsv1.1":
		return tls.VersionTLS11, nil
	case "tlsv1.0":
		return tls.VersionTLS10, nil
	default:
		return tls.VersionTLS12, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "Invalid TLS version specified: %s. Allowed options are TLSv1.0, TLSv1.1, TLSv1.2 & TLSv1.3", tlsVersion)
	}
}

var onceByKeys = sync.Map{}

// ClientConfig returns the TLS config to use for a client to
// connect to a server with the provided parameters.
func ClientConfig(mode SslMode, cert, key, ca, crl, name string, minTLSVersion uint16) (*tls.Config, error) {
	config := &tls.Config{
		MinVersion: minTLSVersion,
	}

	// Load the client-side cert & key if any.
	if cert != "" && key != "" {
		certificates, err := loadTLSCertificate(cert, key)

		if err != nil {
			return nil, err
		}

		config.Certificates = *certificates
	}

	// Load the server CA if any.
	if ca != "" {
		certificatePool, err := loadx509CertPool(ca)

		if err != nil {
			return nil, err
		}

		config.RootCAs = certificatePool
	}

	// Set the server name if any.
	if name != "" {
		config.ServerName = name
	}

	switch mode {
	case Disabled:
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "can't create config for disabled mode")
	case Preferred, Required:
		config.InsecureSkipVerify = true
	case VerifyCA:
		config.InsecureSkipVerify = true
		config.VerifyConnection = func(cs tls.ConnectionState) error {
			caRoots := config.RootCAs
			if caRoots == nil {
				var err error
				caRoots, err = x509.SystemCertPool()
				if err != nil {
					return err
				}
			}
			opts := x509.VerifyOptions{
				Roots:         caRoots,
				Intermediates: x509.NewCertPool(),
			}
			for _, cert := range cs.PeerCertificates[1:] {
				opts.Intermediates.AddCert(cert)
			}
			_, err := cs.PeerCertificates[0].Verify(opts)
			return err
		}
	case VerifyIdentity:
		// Nothing to do here, default config is the strictest and correct.
	default:
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "invalid mode: %s", mode)
	}

	if crl != "" {
		crlFunc, err := verifyPeerCertificateAgainstCRL(crl)
		if err != nil {
			return nil, err
		}
		config.VerifyPeerCertificate = crlFunc
	}

	return config, nil
}

// ServerConfig returns the TLS config to use for a server to
// accept client connections.
func ServerConfig(cert, key, ca, crl, serverCA string, minTLSVersion uint16) (*tls.Config, error) {
	config := &tls.Config{
		MinVersion: minTLSVersion,
	}

	var certificates *[]tls.Certificate
	var err error

	if serverCA != "" {
		certificates, err = combineAndLoadTLSCertificates(serverCA, cert, key)
	} else {
		certificates, err = loadTLSCertificate(cert, key)
	}

	if err != nil {
		return nil, err
	}
	config.Certificates = *certificates

	// if specified, load ca to validate client,
	// and enforce clients present valid certs.
	if ca != "" {
		certificatePool, err := loadx509CertPool(ca)

		if err != nil {
			return nil, err
		}

		config.ClientCAs = certificatePool
		config.ClientAuth = tls.RequireAndVerifyClientCert
	}

	if crl != "" {
		crlFunc, err := verifyPeerCertificateAgainstCRL(crl)
		if err != nil {
			return nil, err
		}
		config.VerifyPeerCertificate = crlFunc
	}

	return config, nil
}

// ServerConfigWithGlobal behaves like ServerConfig but additionally loads a
// second, independently-rooted "global" server certificate and installs an
// SNI-based GetCertificate callback: a client whose TLS SNI (ServerName)
// matches globalSNI is served the global leaf, while every other client is
// served the default (per-DC) leaf that ServerConfig loads. This lets a single
// gRPC listener present two independent server identities without affecting
// existing clients (which never send globalSNI). The client-CA trust store
// (ClientCAs/ClientAuth) is shared by both leaves, so bundling additional CAs
// into `ca` widens accepted client certs for both.
//
// If globalCert/globalKey/globalSNI are empty, it is exactly ServerConfig.
func ServerConfigWithGlobal(cert, key, ca, crl, serverCA, globalCert, globalKey, globalSNI string, minTLSVersion uint16) (*tls.Config, error) {
	config, err := ServerConfig(cert, key, ca, crl, serverCA, minTLSVersion)
	if err != nil {
		return nil, err
	}

	// No global leaf requested: behave exactly like ServerConfig.
	if globalCert == "" && globalKey == "" && globalSNI == "" {
		return config, nil
	}
	// Partial config is a deployment error. Fail fast rather than silently
	// serving only the default leaf, which would leave cross-hublet clients
	// failing later with an opaque TLS handshake error.
	if globalCert == "" || globalKey == "" || globalSNI == "" {
		return nil, vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT,
			"grpc global server cert requires cert, key and SNI all set (cert=%q, key=%q, sni=%q)",
			globalCert, globalKey, globalSNI)
	}

	// Loaded bare: unlike the default leaf (which combines with --grpc-server-ca
	// when set), no CA chain is stapled here, so the global cert file must itself
	// bundle any intermediates it needs.
	globalCertificates, err := loadTLSCertificate(globalCert, globalKey)
	if err != nil {
		return nil, err
	}

	// Capture both leaves up front; GetCertificate must not do I/O on the hot path.
	defaultLeaf := config.Certificates[0]
	globalLeaf := (*globalCertificates)[0]

	config.GetCertificate = func(hello *tls.ClientHelloInfo) (*tls.Certificate, error) {
		if hello.ServerName == globalSNI {
			return &globalLeaf, nil
		}
		return &defaultLeaf, nil
	}

	return config, nil
}

var certPools = sync.Map{}

func loadx509CertPool(ca string) (*x509.CertPool, error) {
	once, _ := onceByKeys.LoadOrStore(ca, &sync.Once{})

	var err error
	once.(*sync.Once).Do(func() {
		err = doLoadx509CertPool(ca)
	})
	if err != nil {
		return nil, err
	}

	result, ok := certPools.Load(ca)

	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_NOT_FOUND, "Cannot find loaded x509 cert pool for ca: %s", ca)
	}

	return result.(*x509.CertPool), nil
}

func doLoadx509CertPool(ca string) error {
	b, err := os.ReadFile(ca)
	if err != nil {
		return vterrors.Errorf(vtrpc.Code_NOT_FOUND, "failed to read ca file: %s", ca)
	}

	cp := x509.NewCertPool()
	if !cp.AppendCertsFromPEM(b) {
		return vterrors.Errorf(vtrpc.Code_UNKNOWN, "failed to append certificates")
	}

	certPools.Store(ca, cp)

	return nil
}

var tlsCertificates = sync.Map{}

func tlsCertificatesIdentifier(tokens ...string) string {
	return strings.Join(tokens, ";")
}

func loadTLSCertificate(cert, key string) (*[]tls.Certificate, error) {
	tlsIdentifier := tlsCertificatesIdentifier(cert, key)
	once, _ := onceByKeys.LoadOrStore(tlsIdentifier, &sync.Once{})

	var err error
	once.(*sync.Once).Do(func() {
		err = doLoadTLSCertificate(cert, key)
	})

	if err != nil {
		return nil, err
	}

	result, ok := tlsCertificates.Load(tlsIdentifier)

	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_NOT_FOUND, "Cannot find loaded tls certificate with cert: %s, key: %s", cert, key)
	}

	return result.(*[]tls.Certificate), nil
}

func doLoadTLSCertificate(cert, key string) error {
	tlsIdentifier := tlsCertificatesIdentifier(cert, key)

	var certificate []tls.Certificate
	// Load the server cert and key.
	crt, err := tls.LoadX509KeyPair(cert, key)
	if err != nil {
		return vterrors.Errorf(vtrpc.Code_NOT_FOUND, "failed to load tls certificate, cert %s, key: %s", cert, key)
	}

	certificate = []tls.Certificate{crt}

	tlsCertificates.Store(tlsIdentifier, &certificate)

	return nil
}

var combinedTLSCertificates = sync.Map{}

func combineAndLoadTLSCertificates(ca, cert, key string) (*[]tls.Certificate, error) {
	combinedTLSIdentifier := tlsCertificatesIdentifier(ca, cert, key)
	once, _ := onceByKeys.LoadOrStore(combinedTLSIdentifier, &sync.Once{})

	var err error
	once.(*sync.Once).Do(func() {
		err = doLoadAndCombineTLSCertificates(ca, cert, key)
	})

	if err != nil {
		return nil, err
	}

	result, ok := combinedTLSCertificates.Load(combinedTLSIdentifier)

	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_NOT_FOUND, "Cannot find loaded tls certificate chain with ca: %s, cert: %s, key: %s", ca, cert, key)
	}

	return result.(*[]tls.Certificate), nil
}

func doLoadAndCombineTLSCertificates(ca, cert, key string) error {
	combinedTLSIdentifier := tlsCertificatesIdentifier(ca, cert, key)

	// Read CA certificates chain
	caB, err := os.ReadFile(ca)
	if err != nil {
		return vterrors.Errorf(vtrpc.Code_NOT_FOUND, "failed to read ca file: %s", ca)
	}

	// Read server certificate
	certB, err := os.ReadFile(cert)
	if err != nil {
		return vterrors.Errorf(vtrpc.Code_NOT_FOUND, "failed to read server cert file: %s", cert)
	}

	// Read server key file
	keyB, err := os.ReadFile(key)
	if err != nil {
		return vterrors.Errorf(vtrpc.Code_NOT_FOUND, "failed to read key file: %s", key)
	}

	// Load CA, server cert and key.
	var certificate []tls.Certificate
	crt, err := tls.X509KeyPair(append(certB, caB...), keyB)
	if err != nil {
		return vterrors.Errorf(vtrpc.Code_NOT_FOUND, "failed to load and merge tls certificate with CA, ca %s, cert %s, key: %s", ca, cert, key)
	}

	certificate = []tls.Certificate{crt}

	combinedTLSCertificates.Store(combinedTLSIdentifier, &certificate)

	return nil
}

// ClearCertificateCache clears the cached certificates for the given paths.
// This allows certificates to be reloaded from disk on the next call to
// ClientConfig or ServerConfig. After calling this function, the next call
// to load certificates will read fresh data from the filesystem.
//
// Parameters:
//   - cert: Path to the certificate file (can be empty)
//   - key: Path to the key file (can be empty)
//   - ca: Path to the CA certificate file (can be empty)
//   - crl: Path to the CRL file (can be empty, currently not cached but included for completeness)
//   - serverCA: Path to the server CA file used for combined certificate loading (can be empty)
//
// Note: This function is thread-safe and can be called concurrently.
func ClearCertificateCache(cert, key, ca, crl, serverCA string) {
	// Clear CA certificate pool cache
	if ca != "" {
		onceByKeys.Delete(ca)
		certPools.Delete(ca)
	}

	// Clear TLS certificate cache (cert + key pair)
	if cert != "" && key != "" {
		tlsIdentifier := tlsCertificatesIdentifier(cert, key)
		onceByKeys.Delete(tlsIdentifier)
		tlsCertificates.Delete(tlsIdentifier)
	}

	// Clear combined TLS certificate cache (used by ServerConfig when serverCA is provided)
	if serverCA != "" && cert != "" && key != "" {
		combinedTLSIdentifier := tlsCertificatesIdentifier(serverCA, cert, key)
		onceByKeys.Delete(combinedTLSIdentifier)
		combinedTLSCertificates.Delete(combinedTLSIdentifier)
	}
}
