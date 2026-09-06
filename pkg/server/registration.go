package server

import (
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"

	sdkcrypto "github.com/TBD54566975/ssi-sdk/crypto"
	didkey "github.com/TBD54566975/ssi-sdk/did/key"
	"github.com/shinzonetwork/shinzo-generator-client/pkg/logger"
)

const (
	registrationMessage = "Shinzo Network Generator registration"
	registrationAppHost = "registration.shinzo.network"
	defaultP2PPort      = "9171"
)

// DisplayRegistration holds the signed registration data for an generator node.
type DisplayRegistration struct {
	Enabled             bool                `json:"enabled"`
	Message             string              `json:"message"`
	DID                 string              `json:"did,omitempty"`
	ConnectionString    string              `json:"connection_string,omitempty"`
	SourceChain         string              `json:"source_chain,omitempty"`
	SourceChainID       uint64              `json:"source_chain_id,omitempty"`
	DefraPKRegistration DefraPKRegistration `json:"defra_pk_registration,omitzero"`
}

// DefraPKRegistration holds the public key and signed message for DefraDB PK registration.
type DefraPKRegistration struct {
	PublicKey   string `json:"public_key,omitempty"`
	SignedPKMsg string `json:"signed_pk_message,omitempty"`
}

// PeerIDRegistration holds the peer ID and signed message for P2P peer registration.
type PeerIDRegistration struct {
	PeerID        string `json:"peer_id,omitempty"`
	SignedPeerMsg string `json:"signed_peer_message,omitempty"`
}

// getRegistrationData returns the signed registration data for the generator.
func (hs *HealthServer) getRegistrationData(r *http.Request) (*DisplayRegistration, error) {
	if hs.indexer == nil {
		return nil, errIndexerNotAvailable
	}

	defraReg, signErr := hs.indexer.SignRegistrationMessage(registrationMessage)
	registration := &DisplayRegistration{
		Enabled: signErr == nil,
		Message: normalizeHex(hex.EncodeToString([]byte(registrationMessage))),
	}
	if signErr != nil {
		return registration, signErr
	}

	registration.DefraPKRegistration = DefraPKRegistration{
		PublicKey:   normalizeHex(defraReg.PublicKey),
		SignedPKMsg: normalizeHex(defraReg.SignedPKMsg),
	}
	if did, err := deriveDID(registration.DefraPKRegistration.PublicKey); err == nil {
		registration.DID = did
	} else {
		logger.Sugar.Debugf("failed to derive registration DID: %v", err)
	}
	if p2p, err := hs.indexer.GetPeerInfo(); err == nil {
		registration.ConnectionString = deriveConnectionString(r, p2p)
	}
	registration.SourceChain, registration.SourceChainID = hs.indexer.GetSourceChainInfo()

	return registration, nil
}

// registrationAppHandler redirects to the registration app with registration data as query params.
func (hs *HealthServer) registrationAppHandler(w http.ResponseWriter, r *http.Request) {
	registration, err := hs.getRegistrationData(r)
	if err != nil || registration == nil || !registration.Enabled {
		http.Error(w, "Registration data not available", http.StatusServiceUnavailable)
		return
	}

	http.Redirect(w, r, buildRegistrationAppURL(registration), http.StatusTemporaryRedirect)
}

func buildRegistrationAppURL(registration *DisplayRegistration) string {
	redirectURL := url.URL{
		Scheme: "https",
		Host:   registrationAppHost,
		Path:   "/generator-registration",
	}
	query := redirectURL.Query()
	query.Set("signedMessage", registration.Message)
	query.Set("defraPublicKey", registration.DefraPKRegistration.PublicKey)
	query.Set("defraPublicKeySignedMessage", registration.DefraPKRegistration.SignedPKMsg)
	query.Set("connectionString", registration.ConnectionString)
	query.Set("sourceChain", registration.SourceChain)
	query.Set("sourceChainId", fmt.Sprintf("%d", registration.SourceChainID))
	redirectURL.RawQuery = query.Encode()

	return redirectURL.String()
}

func deriveDID(publicKeyHex string) (string, error) {
	publicKeyBytes, err := hex.DecodeString(strings.TrimPrefix(normalizeHex(publicKeyHex), "0x"))
	if err != nil {
		return "", fmt.Errorf("decode public key: %w", err)
	}
	didDoc, err := didkey.CreateDIDKey(sdkcrypto.SECP256k1, publicKeyBytes)
	if err != nil {
		return "", fmt.Errorf("create did key: %w", err)
	}
	return didDoc.String(), nil
}

// deriveConnectionString suggests the multiaddr to register, preferring the address the
// request arrived on over the node's own. Returns empty if neither is publicly routable.
func deriveConnectionString(r *http.Request, p2p *P2PInfo) string {
	if p2p == nil || p2p.Self == nil || p2p.Self.ID == "" {
		return ""
	}

	port := p2pPort(p2p.Self.Addresses)
	if ip := requestHostIP(r); ip != "" {
		return fmt.Sprintf("/ip4/%s/tcp/%s/p2p/%s", ip, port, p2p.Self.ID)
	}

	if addr := firstUsableP2PAddress(p2p.Self.Addresses); addr != "" {
		return fmt.Sprintf("%s/p2p/%s", addr, p2p.Self.ID)
	}
	return ""
}

func firstForwardedValue(value string) string {
	if value == "" {
		return ""
	}
	return strings.TrimSpace(strings.Split(value, ",")[0])
}

func requestHostIP(r *http.Request) string {
	if r == nil {
		return ""
	}
	hosts := []string{
		firstForwardedValue(r.Header.Get("X-Forwarded-Host")),
		r.Host,
	}
	for _, host := range hosts {
		if ip := hostIP(host); ip != "" {
			return ip
		}
	}
	return ""
}

func hostIP(host string) string {
	if splitHost, _, err := net.SplitHostPort(host); err == nil {
		host = splitHost
	}
	host = strings.Trim(host, "[]")
	ip := net.ParseIP(host)
	if !isPublicIP4(ip) {
		return ""
	}
	return ip.String()
}

func p2pPort(addresses []string) string {
	for _, addr := range addresses {
		parts := strings.Split(addr, "/")
		for i := 0; i < len(parts)-1; i++ {
			if parts[i] == "tcp" && parts[i+1] != "" {
				return parts[i+1]
			}
		}
	}
	return defaultP2PPort
}

func firstUsableP2PAddress(addresses []string) string {
	for _, addr := range addresses {
		if isUsableIP4Multiaddr(addr) {
			return addr
		}
	}
	return ""
}

func isUsableIP4Multiaddr(addr string) bool {
	parts := strings.Split(addr, "/")
	for i := 0; i < len(parts)-1; i++ {
		if parts[i] != "ip4" {
			continue
		}
		return isPublicIP4(net.ParseIP(parts[i+1]))
	}
	return false
}

// isPublicIP4 reports whether ip is an IPv4 address routable from outside this machine.
func isPublicIP4(ip net.IP) bool {
	if ip == nil || ip.To4() == nil {
		return false
	}
	return !ip.IsLoopback() && !ip.IsUnspecified() && !ip.IsPrivate() && !ip.IsLinkLocalUnicast()
}
