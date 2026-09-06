// Package server provides HTTP handlers for the generator's health, metrics, registration,
// and schema endpoints.
//
// Error response formats differ by audience:
//   - Host-client endpoints (/api/v1/*) return structured JSON errors via writeJSONError,
//     using the errorResponse envelope {error, message}. These are consumed programmatically
//     by other generator clients that need machine-parseable error details.
//   - User-facing endpoints (/, /health, /registration, /metrics, /snapshots) use plain text
//     errors via http.Error. These serve browsers and operational dashboards where plain text
//     is simpler and sufficient.
package server

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/shinzonetwork/shinzo-generator-client/pkg/constants"
	"github.com/shinzonetwork/shinzo-generator-client/pkg/logger"
	"github.com/shinzonetwork/shinzo-generator-client/pkg/snapshot"
	"github.com/sourcenetwork/defradb/node"
)

//go:embed health_status_page.html
var embeddedHealthStatusPageHTML string
var errIndexerNotAvailable = errors.New("indexer not available") //nolint:err113

const (
	// ServerUnhealthyStatus is the status string used when the server is unhealthy.
	ServerUnhealthyStatus = "unhealthy"

	// HealthCheckStaleThreshold is the duration after which a last-processed time is considered stale.
	HealthCheckStaleThreshold = 5 * time.Minute

	// DefraDBCheckTimeout is the timeout for checking DefraDB connectivity.
	DefraDBCheckTimeout = 5 * time.Second

	// ShinzoHubProtoAPIPort is the port for the ShinzoHub Cosmos LCD / protobuf REST API.
	ShinzoHubProtoAPIPort = 1317

	// ShinzoHubCosmosAPIPort is the port for the ShinzoHub Cosmos RPC API.
	ShinzoHubCosmosAPIPort = 25567
)

// ShinzoHubAPIURL builds a full base URL from a hostname and port.
// Returns an empty string when hostname is empty so callers can skip hub queries gracefully.
func ShinzoHubAPIURL(hostname string, port int) string {
	if hostname == "" {
		return ""
	}
	return fmt.Sprintf("http://%s:%d", hostname, port)
}

// HealthServer provides HTTP endpoints for health checks and metrics.
type HealthServer struct {
	server               *http.Server
	mux                  *http.ServeMux
	indexer              HealthChecker
	defraURL             string
	snapshotter          *snapshot.Snapshotter
	defraNode            *node.Node
	startTime            time.Time
	healthStatusPagePath string
	querySnapshotSigsFn  func(ctx context.Context, n *node.Node) (map[string]*snapshot.SnapshotSignatureData, error)
	shinzoHubRESTBase    string // full base URL injected into the health page template, e.g. "http://testnet.shinzo.network:1317"
}

// HealthChecker interface for checking indexer health.
type HealthChecker interface {
	IsHealthy() bool
	GetCurrentBlock() int64
	GetLastProcessedTime() time.Time
	GetPeerInfo() (*P2PInfo, error)
	GetSourceChainInfo() (string, uint64)
	SignRegistrationMessage(message string) (DefraPKRegistration, error)
	SignMessages(message string) (DefraPKRegistration, PeerIDRegistration, error)
}

// P2PInfo represents DefraDB P2P network information.
type P2PInfo struct {
	Enabled  bool       `json:"enabled"`
	Self     *PeerInfo  `json:"self,omitempty"`
	PeerInfo []PeerInfo `json:"peers"`
}

// PeerInfo contains address and identity information for a DefraDB P2P peer.
type PeerInfo struct {
	ID        string   `json:"id"`
	Addresses []string `json:"addresses"`
	PublicKey string   `json:"public_key,omitempty"`
}

// HealthResponse represents the health check response.
type HealthResponse struct {
	Status           string               `json:"status"`
	Timestamp        time.Time            `json:"timestamp"`
	CurrentBlock     int64                `json:"current_block,omitempty"`
	LastProcessed    time.Time            `json:"last_processed,omitzero"`
	DefraDBConnected bool                 `json:"defradb_connected"`
	Uptime           string               `json:"uptime"`
	UptimeSeconds    float64              `json:"uptime_seconds"`
	P2P              *P2PInfo             `json:"p2p,omitempty"`
	Registration     *DisplayRegistration `json:"registration,omitempty"`
	BuildTags        string               `json:"build_tags,omitempty"`
	SchemaType       string               `json:"schema_type,omitempty"`
}

// MetricsResponse represents basic metrics.
type MetricsResponse struct {
	BlocksProcessed   int64     `json:"blocks_processed"`
	CurrentBlock      int64     `json:"current_block"`
	LastProcessedTime time.Time `json:"last_processed_time"`
	Uptime            string    `json:"uptime"`
}

// NewHealthServer creates a new health server.
func NewHealthServer(port int, indexer HealthChecker, defraURL string) *HealthServer {
	mux := http.NewServeMux()

	hs := &HealthServer{
		server: &http.Server{
			Addr:         fmt.Sprintf(":%d", port),
			Handler:      mux,
			ReadTimeout:  10 * time.Second, //nolint:mnd
			WriteTimeout: 5 * time.Minute,  //nolint:mnd // large snapshot files need time to transfer.
		},
		mux:                  mux,
		indexer:              indexer,
		defraURL:             defraURL,
		startTime:            time.Now(),
		healthStatusPagePath: "pkg/server/health_status_page.html",
		querySnapshotSigsFn:  snapshot.QuerySnapshotSignatures,
	}

	// Register routes
	mux.HandleFunc("/health", hs.healthHandler)
	mux.HandleFunc("/registration", hs.registrationHandler)
	mux.HandleFunc("/registration-app", hs.registrationAppHandler)
	mux.HandleFunc("/metrics", hs.metricsHandler)
	mux.HandleFunc("GET /{$}", hs.rootHandler)

	return hs
}

// SetSnapshotter registers the snapshot provider and enables snapshot HTTP endpoints.
func (hs *HealthServer) SetSnapshotter(s *snapshot.Snapshotter) {
	hs.snapshotter = s
	hs.mux.HandleFunc("/snapshots", hs.snapshotsListHandler)
	hs.mux.HandleFunc("/snapshots/", hs.snapshotDownloadHandler)
}

// SetDefraNode sets the DefraDB node reference for import operations.
func (hs *HealthServer) SetDefraNode(n *node.Node) {
	hs.defraNode = n
	hs.mux.HandleFunc("/snapshots/import", hs.snapshotImportHandler)
}

// SetShinzoHubRESTBase sets the ShinzoHub REST base URL injected into the health status page.
// base should be the full base URL including scheme and port, e.g. "http://testnet.shinzo.network:1317".
// Use ShinzoHubAPIURL to build it from a hostname config value.
func (hs *HealthServer) SetShinzoHubRESTBase(base string) {
	hs.shinzoHubRESTBase = base
}

// Start starts the health server.
func (hs *HealthServer) Start() error {
	logger.Sugar.Infof("Starting health server on %s", hs.server.Addr)
	return hs.server.ListenAndServe()
}

// Stop gracefully stops the health server.
func (hs *HealthServer) Stop(ctx context.Context) error {
	logger.Sugar.Info("Stopping health server...")
	return hs.server.Shutdown(ctx)
}

// healthHandler handles liveness probe requests.
func (hs *HealthServer) healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Content negotiation: Default to HTML for browsers, only serve JSON if explicitly requested.
	accept := r.Header.Get("Accept")
	acceptLower := strings.ToLower(accept)

	uptime := time.Since(hs.startTime)

	// Serve JSON only if explicitly requested (Accept contains application/json and not text/html).
	// Otherwise, default to HTML for browser requests.
	if strings.Contains(acceptLower, "text/html") && !strings.Contains(acceptLower, "application/json") {
		// Default to HTML (browser request or Accept header includes text/html).
		htmlContent := hs.getHealthStatusPageHTML()
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(htmlContent)
		return
	}
	// Serve JSON response.
	response := HealthResponse{
		Status:           "healthy",
		Timestamp:        time.Now(),
		DefraDBConnected: hs.checkDefraDB(),
		Uptime:           uptime.String(),
		UptimeSeconds:    uptime.Seconds(),
	}

	if hs.indexer != nil {
		response.CurrentBlock = hs.indexer.GetCurrentBlock()
		response.LastProcessed = hs.indexer.GetLastProcessedTime()
		p2p, _ := hs.indexer.GetPeerInfo()
		response.P2P = p2p

		if !hs.indexer.IsHealthy() {
			response.Status = ServerUnhealthyStatus
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if response.Status == ServerUnhealthyStatus {
		w.WriteHeader(http.StatusServiceUnavailable)
	}
	_ = json.NewEncoder(w).Encode(response)
}

// registrationHandler handles readiness probe requests.
func (hs *HealthServer) registrationHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Check if indexer is ready (has processed at least one block recently).
	ready := true
	if hs.indexer != nil {
		lastProcessed := hs.indexer.GetLastProcessedTime()
		if time.Since(lastProcessed) > HealthCheckStaleThreshold && !lastProcessed.IsZero() {
			ready = false
		}
	}

	defraConnected := hs.checkDefraDB()
	if !defraConnected {
		ready = false
	}

	uptime := time.Since(hs.startTime)
	response := HealthResponse{
		Status:           "ready",
		Timestamp:        time.Now(),
		DefraDBConnected: defraConnected,
		Uptime:           uptime.String(),
		UptimeSeconds:    uptime.Seconds(),
	}

	if hs.indexer != nil {
		response.CurrentBlock = hs.indexer.GetCurrentBlock()
		response.LastProcessed = hs.indexer.GetLastProcessedTime()
		p2p, err := hs.indexer.GetPeerInfo()
		response.P2P = p2p
		if err != nil {
			response.Status = ServerUnhealthyStatus
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		registration, _ := hs.getRegistrationData(r)
		response.Registration = registration
	}

	if !ready {
		response.Status = "not ready"
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// metricsHandler provides basic metrics in JSON format.
func (hs *HealthServer) metricsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	metrics := MetricsResponse{
		Uptime: time.Since(hs.startTime).String(),
	}

	if hs.indexer != nil {
		metrics.CurrentBlock = hs.indexer.GetCurrentBlock()
		metrics.LastProcessedTime = hs.indexer.GetLastProcessedTime()
		metrics.BlocksProcessed = hs.indexer.GetCurrentBlock() // Simplified
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(metrics)
}

// rootHandler handles root requests.
func (hs *HealthServer) rootHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	response := map[string]any{
		"service":   "Shinzo Network Indexer",
		"version":   "1.0.0",
		"status":    "running",
		"timestamp": time.Now(),
		"endpoints": []string{
			"/health 	      - Health probe",
			"/registration  - Registration information",
			"/registration-app - Registration webapp",
			"/metrics 	    - Basic metrics",
			"/snapshots     - List available snapshots",
			"/snapshots/:id - Download a snapshot file",
			"/api/v1/schema           - Full GraphQL schema SDL",
			"/api/v1/schema/{name}    - Collection schema SDL",
			"/api/v1/schema/collections - Collections metadata",
		},
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// snapshotListEntry extends SnapshotInfo with inline signature data.
type snapshotListEntry struct {
	snapshot.SnapshotInfo

	Signed    bool                            `json:"signed"`
	Signature *snapshot.SnapshotSignatureData `json:"signature,omitempty"`
}

// snapshotsListHandler returns a JSON list of available snapshot files with inline signatures.
func (hs *HealthServer) snapshotsListHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if hs.snapshotter == nil {
		http.Error(w, "Snapshots not enabled", http.StatusNotFound)
		return
	}

	infos := hs.snapshotter.ListSnapshots()

	// Query DefraDB for all snapshot signatures, keyed by filename.
	var sigs map[string]*snapshot.SnapshotSignatureData
	if hs.defraNode != nil {
		var err error
		sigs, err = hs.querySnapshotSigsFn(r.Context(), hs.defraNode)
		if err != nil {
			logger.Sugar.Warnf("Failed to query snapshot signatures: %v", err)
		}
	}

	entries := make([]snapshotListEntry, len(infos))
	for i, info := range infos {
		sig := sigs[info.Filename]
		entries[i] = snapshotListEntry{
			SnapshotInfo: info,
			Signed:       sig != nil,
			Signature:    sig,
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"snapshots": entries,
		"count":     len(entries),
	})
}

// snapshotDownloadHandler serves a snapshot file by name.
// URL: /snapshots/{filename} — serves .jsonl.gz snapshot file.
func (hs *HealthServer) snapshotDownloadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if hs.snapshotter == nil {
		http.Error(w, "Snapshots not enabled", http.StatusNotFound)
		return
	}

	filename := strings.TrimPrefix(r.URL.Path, "/snapshots/")
	if filename == "" {
		hs.snapshotsListHandler(w, r)
		return
	}

	filePath := hs.snapshotter.GetSnapshotPath(filename)
	if filePath == "" {
		http.Error(w, "Snapshot not found", http.StatusNotFound)
		return
	}

	f, err := os.Open(filepath.Clean(filePath))
	if err != nil {
		http.Error(w, "Failed to open file", http.StatusInternalServerError)
		return
	}
	defer func() { _ = f.Close() }()

	stat, err := f.Stat()
	if err != nil {
		http.Error(w, "Failed to stat file", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", stat.Size()))
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))

	written, err := io.Copy(w, f)
	if err != nil {
		logger.Sugar.Errorf("Snapshot download error for %s: %v (wrote %d/%d bytes)", filename, err, written, stat.Size())
	} else {
		logger.Sugar.Infof("Snapshot served: %s (%d bytes)", filename, written)
	}
}

// snapshotImportHandler imports a snapshot file by name.
// POST /snapshots/import?file=snapshot_X_Y.kvsnap.gz.
func (hs *HealthServer) snapshotImportHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if hs.defraNode == nil {
		http.Error(w, "Import not available (no embedded DefraDB)", http.StatusServiceUnavailable)
		return
	}
	if hs.snapshotter == nil {
		http.Error(w, "Snapshots not enabled", http.StatusNotFound)
		return
	}

	filename := r.URL.Query().Get("file")
	if filename == "" {
		http.Error(w, "Missing 'file' query parameter", http.StatusBadRequest)
		return
	}

	filePath := hs.snapshotter.GetSnapshotPath(filename)
	if filePath == "" {
		http.Error(w, "Snapshot not found", http.StatusNotFound)
		return
	}

	result, err := snapshot.ImportKV(r.Context(), hs.defraNode, filePath)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":  err.Error(),
			"result": result, //nolint:goconst
		})
		return
	}

	// Rebuild secondary indexes after bulk KV import.
	// ImportRawKVs writes raw KV pairs directly to the rootstore, bypassing
	// the document layer. Index entries are not included in the export, so
	// we must rebuild them from the imported document data.
	if rebuildErr := snapshot.RebuildAllIndexes(r.Context(), hs.defraNode, constants.DefaultCollections()); rebuildErr != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":  rebuildErr.Error(),
			"result": result,
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": "ok",
		"result": result,
	})
}

// graphQLPath is DefraDB's query endpoint.
const graphQLPath = "/api/v0/graphql"

// graphQLProbe is the query the health check sends. It introspects the schema, so it
// needs no collection.
const graphQLProbe = `{"query":"{ __schema { queryType { name } } }"}`

// defraQueryURL builds the GraphQL endpoint URL for a configured DefraDB address.
//
// The address is a listen address, so it may carry no scheme and may be a wildcard such
// as 0.0.0.0. A request needs both filled in: http is assumed, and a wildcard is reached
// through loopback.
func defraQueryURL(addr string) string {
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	u, err := url.Parse(addr)
	if err != nil {
		return addr + graphQLPath
	}

	if host, port, splitErr := net.SplitHostPort(u.Host); splitErr == nil {
		switch host {
		case "0.0.0.0", "::", "":
			u.Host = net.JoinHostPort("127.0.0.1", port)
		}
	}
	// Append so an address with a path prefix keeps it.
	u.Path = strings.TrimSuffix(u.Path, "/") + graphQLPath
	return u.String()
}

// checkDefraDB reports whether DefraDB answers a GraphQL query. GraphQL reports failures
// in the body, so the status code is not a useful signal.
func (hs *HealthServer) checkDefraDB() bool {
	if hs.defraURL == "" {
		return true // No HTTP API to probe.
	}

	client := &http.Client{Timeout: DefraDBCheckTimeout}
	resp, err := client.Post(defraQueryURL(hs.defraURL), "application/json", strings.NewReader(graphQLProbe))
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()

	// Decoding into a map rejects any body that is not a JSON object.
	var body map[string]json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return false
	}
	_, hasData := body["data"]
	_, hasErrors := body["errors"]
	return hasData || hasErrors
}

// normalizeHex ensures a string is represented as a 0x-prefixed hex string.
// If the string is empty, it is returned unchanged.
func normalizeHex(s string) string {
	if s == "" {
		return s
	}
	if strings.HasPrefix(s, "0x") || strings.HasPrefix(s, "0X") {
		// Normalize any 0X to 0x for consistency.
		return "0x" + s[2:]
	}
	return "0x" + s
}

// getHealthStatusPageHTML reads the HTML template and renders it with runtime config values.
func (hs *HealthServer) getHealthStatusPageHTML() []byte {
	raw := hs.loadHealthStatusPageTemplate()
	rendered := strings.ReplaceAll(string(raw), "{{SHINZOHUB_REST_BASE}}", hs.shinzoHubRESTBase)
	return []byte(rendered)
}

// loadHealthStatusPageTemplate reads the raw HTML template from disk at runtime, falling back to
// the embedded version. Disk reads allow hot-reloading during development without rebuilding.
func (hs *HealthServer) loadHealthStatusPageTemplate() []byte {
	possiblePaths := []string{
		hs.healthStatusPagePath,
		filepath.Join(".", "health_status_page.html"),
	}

	for _, path := range possiblePaths {
		if data, err := os.ReadFile(filepath.Clean(path)); err == nil {
			logger.Sugar.Debugf("Loaded health status page from: %s", path)
			return data
		}
	}

	logger.Sugar.Debug("Using embedded health status page")
	return []byte(embeddedHealthStatusPageHTML)
}
