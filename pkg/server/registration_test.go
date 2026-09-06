package server

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

// containerP2P is a node that only knows loopback and Docker bridge addresses.
func containerP2P() *P2PInfo {
	return &P2PInfo{
		Enabled: true,
		Self: &PeerInfo{
			ID:        testIndexerPeerID,
			Addresses: []string{"/ip4/127.0.0.1/tcp/9171", "/ip4/172.17.0.2/tcp/9171"},
		},
	}
}

func TestIsPublicIP4(t *testing.T) {
	t.Parallel()

	public := []string{"65.21.94.184", "34.63.186.249", "8.8.8.8", "172.15.0.1", "172.32.0.1"}
	for _, addr := range public {
		assert.True(t, isPublicIP4(net.ParseIP(addr)), addr)
	}

	unreachable := []string{
		"127.0.0.1",      // loopback
		"0.0.0.0",        // unspecified
		"172.17.0.2",     // docker bridge
		"172.16.0.1",     // RFC1918 lower bound
		"172.31.255.255", // RFC1918 upper bound
		"10.128.0.90",    // RFC1918
		"192.168.48.2",   // RFC1918
		"169.254.1.1",    // link-local
	}
	for _, addr := range unreachable {
		assert.False(t, isPublicIP4(net.ParseIP(addr)), addr)
	}

	assert.False(t, isPublicIP4(nil))
	assert.False(t, isPublicIP4(net.ParseIP("2001:db8::1")), "IPv4 only")
}

func TestHostIP(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "65.21.94.184", hostIP("65.21.94.184"))
	assert.Equal(t, "65.21.94.184", hostIP("65.21.94.184:8080"))

	assert.Empty(t, hostIP("localhost:8080"), "hostnames carry no address")
	assert.Empty(t, hostIP("example.com"), "hostnames carry no address")
	assert.Empty(t, hostIP("172.17.0.2:8080"), "container address is not reachable off-host")
	assert.Empty(t, hostIP("10.128.0.90:8080"), "LAN address is not reachable off-host")
	assert.Empty(t, hostIP(""))
}

func TestIsUsableIP4Multiaddr(t *testing.T) {
	t.Parallel()

	assert.True(t, isUsableIP4Multiaddr("/ip4/65.21.94.184/tcp/9171"))

	assert.False(t, isUsableIP4Multiaddr("/ip4/172.17.0.2/tcp/9171"))
	assert.False(t, isUsableIP4Multiaddr("/ip4/127.0.0.1/tcp/9171"))
	assert.False(t, isUsableIP4Multiaddr("/ip4/0.0.0.0/tcp/9171"))
	assert.False(t, isUsableIP4Multiaddr("/dns4/65.21.94.184/tcp/9171"), "only ip4 is handled")
	assert.False(t, isUsableIP4Multiaddr(""))
}

// Localhost request against a containerised node: neither source has a routable address.
func TestDeriveConnectionString_ContainerOnlyAddresses(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest(http.MethodGet, "/registration", nil)
	r.Host = "localhost:8080"

	assert.Empty(t, deriveConnectionString(r, containerP2P()))
}

func TestDeriveConnectionString_PublicRequestHost(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest(http.MethodGet, "/registration", nil)
	r.Host = "65.21.94.184:8080"
	p2p := &P2PInfo{
		Enabled: true,
		Self: &PeerInfo{
			ID:        testIndexerPeerID,
			Addresses: []string{"/ip4/198.51.100.7/tcp/9171", "/ip4/172.17.0.2/tcp/9171"},
		},
	}

	assert.Equal(t,
		"/ip4/65.21.94.184/tcp/9171/p2p/"+testIndexerPeerID,
		deriveConnectionString(r, p2p),
		"request address wins over the node's own; port comes from the node's listen address")
}

// Behind a proxy both sources carry an address, and the forwarded one is the client-facing side.
func TestDeriveConnectionString_ForwardedHostPreferred(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest(http.MethodGet, "/registration", nil)
	r.Host = "203.0.113.9:8080"
	r.Header.Set("X-Forwarded-Host", "65.21.94.184")

	assert.Equal(t, "/ip4/65.21.94.184/tcp/9171/p2p/"+testIndexerPeerID, deriveConnectionString(r, containerP2P()))
}

// With host networking the node knows its public address, so a localhost request still works.
func TestDeriveConnectionString_PublicNodeAddress(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest(http.MethodGet, "/registration", nil)
	r.Host = "localhost:8080"
	p2p := &P2PInfo{
		Enabled: true,
		Self: &PeerInfo{
			ID:        testIndexerPeerID,
			Addresses: []string{"/ip4/127.0.0.1/tcp/9171", "/ip4/65.21.94.184/tcp/9171"},
		},
	}

	assert.Equal(t, "/ip4/65.21.94.184/tcp/9171/p2p/"+testIndexerPeerID, deriveConnectionString(r, p2p))
}
