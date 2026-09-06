package indexer

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/shinzonetwork/shinzo-generator-client/config"
	"github.com/shinzonetwork/shinzo-generator-client/pkg/constants"
	"github.com/shinzonetwork/shinzo-generator-client/pkg/defra"
	"github.com/shinzonetwork/shinzo-generator-client/pkg/defradb"
	indexerErrors "github.com/shinzonetwork/shinzo-generator-client/pkg/errors"
	"github.com/shinzonetwork/shinzo-generator-client/pkg/logger"
	"github.com/shinzonetwork/shinzo-generator-client/pkg/pruner"
	"github.com/shinzonetwork/shinzo-generator-client/pkg/rpc"
	"github.com/shinzonetwork/shinzo-generator-client/pkg/schema"
	"github.com/shinzonetwork/shinzo-generator-client/pkg/server"
	"github.com/shinzonetwork/shinzo-generator-client/pkg/signer"
	"github.com/shinzonetwork/shinzo-generator-client/pkg/snapshot"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/sourcenetwork/defradb/client"
	"github.com/sourcenetwork/defradb/node"
)

var (
	// ErrMTLSNotImplemented is returned when the mTLS authentication mode is configured but not yet supported.
	ErrMTLSNotImplemented = errors.New("mTLS auth mode is not yet implemented")
	// ErrUnknownAuthMode is returned when an unrecognized schema authentication mode is provided.
	ErrUnknownAuthMode = errors.New("unknown auth mode")
)

const (
	// ShortDelayTime is a short delay duration used in various places to give time for operations to complete.
	ShortDelayTime = 500 * time.Millisecond // Give the server time to start.
	// DefaultBlocksToIndexAtOnce is the default number of blocks to index concurrently.
	DefaultBlocksToIndexAtOnce = 10
	// DefaultRetryAttempts is the default number of retry attempts for failed operations.
	DefaultRetryAttempts = 3
	// DefaultRetryDelay is the default delay between retry attempts.
	DefaultRetryDelay = 10 * time.Second
	// DefaultSchemaWaitTimeout is the timeout for waiting for the schema to be applied.
	DefaultSchemaWaitTimeout = 15 * time.Second
	// DefaultDefraReadyTimeout is the timeout for waiting for DefraDB to become ready.
	DefaultDefraReadyTimeout = 30 * time.Second
	// DefaultBlockOffset is the number of blocks behind the latest block to process.
	// This prevents "transaction type not supported" errors from very recent blocks.
	DefaultBlockOffset = 3
	// DefaultWorkersAhead is the number of blocks ahead of the last committed block that the processor will allow itself to get before throttling dispatch.
	DefaultWorkersAhead = 2
)

// var requiredPeers = []string{} // Here, we can consider adding any "big peers" we need - these requiredPeers can be used as a quick start point to speed up the peer discovery process.

// defaultListenAddress is the default P2P listen address for the embedded DefraDB node.
const defaultListenAddress string = "/ip4/127.0.0.1/tcp/9171"

// ChainIndexer is the main indexer that processes blockchain blocks.
type ChainIndexer struct {
	cfg                       *config.Config
	collections               *constants.CollectionNames
	shouldIndex               bool
	isStarted                 bool
	hasIndexedAtLeastOneBlock bool
	defraNode                 *node.Node              // Embedded DefraDB node (nil if using external)
	networkHandler            *defradb.NetworkHandler // P2P network handler (nil if using external)
	healthServer              *server.HealthServer
	pruner                    *pruner.Pruner        // Document pruner for removing old blocks.
	snapshotter               *snapshot.Snapshotter // Snapshot exporter for archiving blocks.
	currentBlock              int64
	lastProcessedTime         time.Time
	mutex                     sync.RWMutex
}

// IsStarted returns true if the indexer has been started.
func (i *ChainIndexer) IsStarted() bool {
	return i.isStarted
}

// HasIndexedAtLeastOneBlock returns true if at least one block has been indexed.
func (i *ChainIndexer) HasIndexedAtLeastOneBlock() bool {
	return i.hasIndexedAtLeastOneBlock
}

// GetDefraDBPort returns the port of the embedded DefraDB node, or -1 if using external DefraDB.
func (i *ChainIndexer) GetDefraDBPort() int {
	if i.defraNode == nil {
		return -1
	}
	return defra.GetPort(i.defraNode)
}

// CreateIndexer creates a new ChainIndexer with the provided configuration.
func CreateIndexer(cfg *config.Config) (*ChainIndexer, error) {
	if cfg == nil {
		return nil, indexerErrors.NewConfigurationError(
			"indexer",
			"CreateIndexer",
			"config is nil",
			"host=nil, port=nil",
			nil,
			indexerErrors.WithMetadata("host", "nil"),
			indexerErrors.WithMetadata("port", "nil"))
	}
	return &ChainIndexer{
		cfg:                       cfg,
		collections:               constants.NewCollectionNames(chainPrefixFromConfig(cfg)),
		shouldIndex:               false,
		isStarted:                 false,
		hasIndexedAtLeastOneBlock: false,
	}, nil
}

// chainPrefixFromConfig returns the collection name prefix for the configured chain.
// Falls back to the default Ethereum mainnet prefix for backward compatibility.
func chainPrefixFromConfig(cfg *config.Config) string {
	if cfg == nil {
		return constants.DefaultCollectionPrefix
	}
	name := cfg.Chain.Name
	network := cfg.Chain.Network
	if name == "" {
		name = "Ethereum"
	}
	if network == "" {
		network = "Mainnet"
	}
	return fmt.Sprintf("%s__%s", name, network)
}

// StartIndexing initializes dependencies and starts concurrent block indexing.
func (i *ChainIndexer) StartIndexing(defraStarted bool) error {
	ctx := context.Background()
	cfg := i.cfg

	if cfg == nil {
		return fmt.Errorf("configuration is required - use config.LoadConfig() to load configuration")
	}

	cfg.DefraDB.P2P.BootstrapPeers = append(cfg.DefraDB.P2P.BootstrapPeers, []string{
		// Add any "big peers" here to speed up peer discovery.
	}...)

	if logger.Sugar == nil {
		logger.Init(cfg.Logger.Development)
	}

	logger.Sugar.Infof("Indexing chain: %s (prefix: %s)", cfg.Chain.Name+"__"+cfg.Chain.Network, chainPrefixFromConfig(cfg))

	ctx, err := i.initDefra(ctx, cfg, defraStarted)
	if err != nil {
		return err
	}

	blockHandler, ethClient, err := i.initClients(cfg)
	if err != nil {
		return err
	}
	defer func() { _ = ethClient.Close() }()

	nextBlockToProcess, err := i.resolveStartHeight(ctx, cfg, blockHandler, ethClient)
	if err != nil {
		return err
	}

	i.shouldIndex = true
	logger.Sugar.Info("Starting indexer - will process latest blocks from Geth ", cfg.Geth.NodeURL)

	if err := i.initServices(ctx, cfg, blockHandler); err != nil {
		return err
	}

	if cfg.Indexer.ConcurrentBlocks >= 1 && i.defraNode != nil {
		logger.Sugar.Infof("Using concurrent block processing with %d workers", cfg.Indexer.ConcurrentBlocks)
		return i.runConcurrentIndexing(ctx, ethClient, blockHandler, nextBlockToProcess, cfg)
	}
	return nil
}

// initDefra starts or connects to DefraDB and returns an updated context with identity.
func (i *ChainIndexer) initDefra(ctx context.Context, cfg *config.Config, defraStarted bool) (context.Context, error) {
	if !defraStarted {

		logger.Sugar.Debugf("P2P config: ListenAddr: '%s', BootstrapPeers: %v, Enabled: %t",
			cfg.DefraDB.P2P.ListenAddr, cfg.DefraDB.P2P.BootstrapPeers, cfg.DefraDB.P2P.Enabled)

		var replicationFilter client.ReplicationFilter
		if !cfg.DefraDB.P2P.AcceptIncoming {
			replicationFilter = &indexerReplicationFilter{}
		}

		defraNode, networkHandler, err := defradb.StartDefraInstance(cfg,
			defradb.NewSchemaApplierFromDir(chainPrefixFromConfig(cfg)), nil, replicationFilter, i.collections.AllCollections()...)
		if err != nil {
			return ctx, fmt.Errorf("failed to start DefraDB instance: %w", err)
		}
		i.defraNode = defraNode
		i.networkHandler = networkHandler

		if err := defra.WaitForDefraDB(defraNode.APIURL); err != nil {
			return ctx, err
		}

		// Get the identity context for block signing
		identityCtx, err := defradb.GetIdentityContext(ctx, cfg)
		if err != nil {
			logger.Sugar.Warnf("Failed to get identity context for block signing: %v (block signatures may not work)", err)
		} else {
			ctx = identityCtx
			logger.Sugar.Info("Identity context initialized for block signing")
		}
	} else {
		if err := defra.WaitForDefraDB(cfg.DefraDB.URL); err != nil {
			return ctx, err
		}
		if err := defradb.ApplyCollectionSchemasViaHTTP(ctx, cfg.DefraDB.URL, chainPrefixFromConfig(cfg)); err != nil {
			return ctx, fmt.Errorf("failed to apply schema to external DefraDB: %w", err)
		}
	}

	if i.defraNode == nil {
		return ctx, fmt.Errorf("defraNode is required - external DefraDB via HTTP is no longer supported")
	}

	return ctx, nil
}

// initClients creates the block handler and Ethereum client.
func (i *ChainIndexer) initClients(cfg *config.Config) (*defra.BlockHandler, *rpc.EthereumClient, error) {
	blockHandler, err := defra.NewBlockHandler(i.defraNode, cfg.Indexer.MaxDocsPerTxn, i.collections)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create block handler: %w", err)
	}
	blockHandler.SetBatchSizes(cfg.Indexer.MaxTxDocsPerBatch, cfg.Indexer.MaxLogDocsPerBatch, cfg.Indexer.MaxALEDocsPerBatch)
	logger.Sugar.Infof("Using direct DB access for embedded DefraDB (maxDocsPerTxn=%d, txBatch=%d, logBatch=%d, aleBatch=%d)",
		cfg.Indexer.MaxDocsPerTxn, cfg.Indexer.MaxTxDocsPerBatch, cfg.Indexer.MaxLogDocsPerBatch, cfg.Indexer.MaxALEDocsPerBatch)

	ethClient, err := rpc.NewEthereumClient(cfg.Geth.NodeURL, cfg.Geth.WsURL, cfg.Geth.APIKey, cfg.Geth.APIKeyType)
	if err != nil {
		logCtx := indexerErrors.LogContext(err)
		logger.Sugar.With("context", logCtx).Fatalf("Failed to connect to Ethereum client: %v", err)
	}

	return blockHandler, ethClient, nil
}

// resolveStartHeight determines the block number to start indexing from.
func (i *ChainIndexer) resolveStartHeight(ctx context.Context, cfg *config.Config, blockHandler *defra.BlockHandler, ethClient *rpc.EthereumClient) (int64, error) {
	configuredHeight := int64(cfg.Indexer.StartHeight)
	var highestExisting int64
	var pruneQueue *pruner.IndexerQueue

	if cfg.Pruner.Enabled {
		pruneQueue = pruner.NewIndexerQueue()
		queueFilePath := filepath.Join(cfg.DefraDB.Store.Path, "prune_queue.gob")
		loaded, err := pruneQueue.LoadFromFile(queueFilePath)
		if err != nil {
			logger.Sugar.Warnf("Failed to load prune queue from disk: %v", err)
		} else if loaded > 0 {
			logger.Sugar.Infof("Restored %d entries from prune queue file", loaded)
		}
		highestExisting = pruneQueue.HighestBlockNumber()
	}

	if highestExisting == 0 {
		nBlock, err := blockHandler.GetHighestBlockNumber(ctx)
		if err != nil {
			logger.Sugar.Debugf("No existing blocks found in DB: %v", err)
		} else {
			highestExisting = nBlock
		}
	}

	latestBlock, err := ethClient.GetLatestBlockNumber(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get latest block number from RPC: %w", err)
	}
	chainTip := latestBlock.Int64()
	startBuffer := int64(cfg.Indexer.StartBuffer)

	switch {
	case highestExisting > 0:
		resumeFrom := highestExisting + 1
		gap := chainTip - highestExisting
		if gap > startBuffer {
			resumeFrom = chainTip - startBuffer
			logger.Sugar.Infof("Gap of %d blocks, skipping ahead to %d (chain tip: %d)", gap, resumeFrom, chainTip)
		}
		cfg.Indexer.StartHeight = int(resumeFrom)
		logger.Sugar.Infof("Resuming from block %d (highest existing: %d, chain tip: %d)", cfg.Indexer.StartHeight, highestExisting, chainTip)
	case configuredHeight > 0:
		logger.Sugar.Infof("Starting from configured height %d (chain tip: %d)", configuredHeight, chainTip)
	default:
		cfg.Indexer.StartHeight = max(int(chainTip-startBuffer), 0)
		logger.Sugar.Infof("No existing blocks, starting from %d (chain tip: %d)", cfg.Indexer.StartHeight, chainTip)
	}

	return int64(cfg.Indexer.StartHeight), nil
}

// newSchemaAuthenticator builds the authenticator for the schema endpoint and
// warns when token auth is fail-closed with zero API keys configured.
func newSchemaAuthenticator(cfg *config.Config) (server.Authenticator, error) {
	auth, err := newAuthenticator(cfg.Indexer.SchemaAuthMode, cfg.Indexer.SchemaAPIKeys)
	if err != nil {
		return nil, fmt.Errorf("schema auth configuration error: %w", err)
	}
	if (cfg.Indexer.SchemaAuthMode == constants.SchemaAuthModeToken || cfg.Indexer.SchemaAuthMode == "") && len(cfg.Indexer.SchemaAPIKeys) == 0 {
		logger.Sugar.Warn("schema auth is fail-closed with zero API keys configured — " +
			"all schema requests will return 503. Set SCHEMA_API_KEYS env var (comma-separated) " +
			"or set SCHEMA_AUTH_MODE=none to disable token auth")
	}
	return auth, nil
}

// initServices starts the health server, pruner, and snapshotter if configured.
func (i *ChainIndexer) initServices(ctx context.Context, cfg *config.Config, blockHandler *defra.BlockHandler) error {
	if cfg.Indexer.HealthServerPort > 0 {
		if err := i.initHealthServer(cfg); err != nil {
			return err
		}
	}

	if cfg.Pruner.Enabled && i.defraNode != nil {
		i.pruner = pruner.NewPruner(&cfg.Pruner, i.defraNode)
		pruneQueue := pruner.NewIndexerQueue()
		// Binds the queue to its file before anything tracks into it. Save is a no-op until this
		// runs, so without it the queue is never written and never survives a restart.
		queueFilePath := filepath.Join(cfg.DefraDB.Store.Path, "prune_queue.gob")
		restored, err := pruneQueue.LoadFromFile(queueFilePath)
		if err != nil {
			logger.Sugar.Warnf("Failed to load prune queue from disk: %v", err)
		} else if restored > 0 {
			logger.Sugar.Infof("Restored %d entries from prune queue file", restored)
		}
		i.pruner.SetQueue(pruneQueue)
		blockHandler.SetDocIDTracker(&indexerQueueTracker{
			queue:       pruneQueue,
			collections: i.collections,
		})
		logger.Sugar.Infof("Prune queue ready (queue=%d, max_blocks=%d)", pruneQueue.Len(), cfg.Pruner.MaxBlocks)
		if err := i.pruner.Start(ctx); err != nil {
			logger.Sugar.Warnf("Failed to start pruner: %v", err)
		}
	}

	if cfg.Snapshot.Enabled && i.defraNode != nil {
		i.snapshotter = snapshot.New(&cfg.Snapshot, i.defraNode)
		if err := i.snapshotter.Start(ctx); err != nil {
			logger.Sugar.Warnf("Failed to start snapshotter: %v", err)
		}
		if i.healthServer != nil {
			i.healthServer.SetSnapshotter(i.snapshotter)
		}
	}

	return nil
}

// initHealthServer creates and starts the health server with schema and hub endpoints configured.
func (i *ChainIndexer) initHealthServer(cfg *config.Config) error {
	// The configured address is rewritten before the API binds, so probe the bound address.
	var healthDefraURL string
	if i.defraNode != nil {
		healthDefraURL = i.defraNode.APIURL
	}
	i.healthServer = server.NewHealthServer(cfg.Indexer.HealthServerPort, i, healthDefraURL)
	if i.defraNode != nil {
		i.healthServer.SetDefraNode(i.defraNode)
	}
	if cfg.Chain.Hub != "" {
		i.healthServer.SetShinzoHubRESTBase(server.ShinzoHubAPIURL(cfg.Chain.Hub, server.ShinzoHubProtoAPIPort))
	}

	auth, err := newSchemaAuthenticator(cfg)
	if err != nil {
		return err
	}
	prefix := chainPrefixFromConfig(cfg)
	sdl, err := schema.GetSchemaForChain(prefix)
	if err != nil {
		return fmt.Errorf("load schema for chain %s: %w", prefix, err)
	}
	if err := i.healthServer.EnableSchemaEndpoint(sdl, prefix, auth); err != nil {
		return fmt.Errorf("enable schema endpoint: %w", err)
	}
	go func() {
		if err := i.healthServer.Start(); err != nil {
			logger.Sugar.Errorf("Health server failed: %v", err)
		}
	}()
	if cfg.Indexer.OpenBrowserOnStart {
		go func() {
			time.Sleep(ShortDelayTime)
			openBrowser(fmt.Sprintf("http://localhost:%d/health", cfg.Indexer.HealthServerPort))
			logger.Sugar.Infof("Opened health page in browser")
		}()
	}
	return nil
}

// runConcurrentIndexing runs the indexer with concurrent block processing.
func (i *ChainIndexer) runConcurrentIndexing(
	ctx context.Context,
	ethClient *rpc.EthereumClient,
	blockHandler *defra.BlockHandler,
	startBlock int64,
	cfg *config.Config,
) error {
	i.shouldIndex = true
	i.isStarted = true

	processor := NewConcurrentBlockProcessor(
		blockHandler,
		ethClient,
		cfg.Indexer.ConcurrentBlocks,
		cfg.Indexer.ReceiptWorkers,
		cfg.Indexer.BlocksPerMinute,
	)

	return processor.ProcessBlocks(ctx, startBlock, func(blockNum int64) {
		i.updateBlockInfo(blockNum)
		i.hasIndexedAtLeastOneBlock = true
	})
}

// StopIndexing halts the indexer and cleanly shuts down all subsystems.
func (i *ChainIndexer) StopIndexing() {
	i.shouldIndex = false
	i.isStarted = false

	// Stop snapshotter before pruner (capture data before it's pruned)
	if i.snapshotter != nil {
		i.snapshotter.Stop()
		i.snapshotter = nil
	}

	// Stop pruner
	if i.pruner != nil {
		i.pruner.Stop()
		i.pruner = nil
	}

	// Stop health server
	if i.healthServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), DefaultRetryDelay)
		defer cancel()
		_ = i.healthServer.Stop(ctx)
	}

	// Stop P2P network handler before closing the node
	if i.networkHandler != nil {
		ctx, cancel := context.WithTimeout(context.Background(), DefaultRetryDelay)
		defer cancel()
		_ = i.networkHandler.StopNetwork(&ctx)
		i.networkHandler = nil
	}

	// Close embedded DefraDB node if it exists
	if i.defraNode != nil {
		_ = i.defraNode.Close(context.Background())
		i.defraNode = nil
	}
}

// IsHealthy returns true if the indexer is running and has processed blocks recently.
func (i *ChainIndexer) IsHealthy() bool {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	// Consider healthy if started and processed at least one block recently
	if !i.isStarted {
		return false
	}

	// If we've never processed a block, we're still healthy (starting up)
	if i.lastProcessedTime.IsZero() {
		return true
	}

	// Consider unhealthy if no blocks processed in last 10 minutes
	return time.Since(i.lastProcessedTime) < 10*time.Minute
}

// GetCurrentBlock returns the last processed block number.
func (i *ChainIndexer) GetCurrentBlock() int64 {
	i.mutex.RLock()
	defer i.mutex.RUnlock()
	return i.currentBlock
}

// GetLastProcessedTime returns the time at which the last block was processed.
func (i *ChainIndexer) GetLastProcessedTime() time.Time {
	i.mutex.RLock()
	defer i.mutex.RUnlock()
	return i.lastProcessedTime
}

// GetPeerInfo returns DefraDB P2P network information.
func (i *ChainIndexer) GetPeerInfo() (*server.P2PInfo, error) {
	i.mutex.RLock()
	defer i.mutex.RUnlock()

	// If no embedded DefraDB node, return nil.
	if i.defraNode == nil {
		return nil, fmt.Errorf("defra is nil - peer info not available for external DefraDB")
	}

	ctx := context.Background()

	// Use NetworkHandler to determine if P2P is active.
	networkActive := i.networkHandler != nil && i.networkHandler.IsNetworkActive()

	// Get this node's own peer info (listening addresses).
	ownAddresses, err := i.defraNode.DB.PeerInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("error fetching own peer info: %w", err)
	}
	ownPeers, _ := defradb.BootstrapIntoPeers(ownAddresses)

	var selfInfo *server.PeerInfo
	if len(ownPeers) > 0 {
		// Collect all addresses for our own peer ID.
		var addresses []string
		for _, p := range ownPeers {
			addresses = append(addresses, p.Addresses...)
		}
		selfInfo = &server.PeerInfo{
			ID:        ownPeers[0].ID,
			Addresses: addresses,
			PublicKey: extractPublicKeyFromPeerID(ownPeers[0].ID),
		}
	}

	// Get actually connected peers (may fail if P2P is not initialized).
	activePeerStrings, err := i.defraNode.DB.ActivePeers(ctx)
	if err != nil {
		activePeerStrings = nil // P2P not available, treat as no peers.
	}
	activePeers, _ := defradb.BootstrapIntoPeers(activePeerStrings)

	// Deduplicate peers by ID and merge addresses.
	peerMap := make(map[string]*server.PeerInfo)
	for _, peer := range activePeers {
		if existing, ok := peerMap[peer.ID]; ok {
			existing.Addresses = append(existing.Addresses, peer.Addresses...)
		} else {
			peerMap[peer.ID] = &server.PeerInfo{
				ID:        peer.ID,
				Addresses: peer.Addresses,
				PublicKey: extractPublicKeyFromPeerID(peer.ID),
			}
		}
	}
	serverPeerInfo := make([]server.PeerInfo, 0, len(peerMap))
	for _, p := range peerMap {
		serverPeerInfo = append(serverPeerInfo, *p)
	}

	return &server.P2PInfo{
		Self:     selfInfo,
		PeerInfo: serverPeerInfo,
		Enabled:  networkActive,
	}, nil
}

// extractPublicKeyFromPeerID attempts to extract the public key from a libp2p PeerID.
func extractPublicKeyFromPeerID(peerID string) string {
	// Parse the PeerID string into a libp2p peer.ID
	id, err := peer.Decode(peerID)
	if err != nil {
		logger.Sugar.Warnf("Failed to decode PeerID %s: %v", peerID, err)
		return ""
	}

	// Extract the public key from the PeerID.
	pubKey, err := id.ExtractPublicKey()
	if err != nil {
		logger.Sugar.Warnf("Failed to extract public key from PeerID %s: %v", peerID, err)
		return ""
	}

	// Convert public key to bytes and then to hex string.
	pubKeyBytes, err := pubKey.Raw()
	if err != nil {
		logger.Sugar.Warnf("Failed to get raw bytes from public key: %v", err)
		return ""
	}

	// Return hex-encoded public key.
	return hex.EncodeToString(pubKeyBytes)
}

// updateBlockInfo updates the current block and last processed time.
func (i *ChainIndexer) updateBlockInfo(blockNum int64) {
	i.mutex.Lock()
	defer i.mutex.Unlock()
	i.currentBlock = blockNum
	i.lastProcessedTime = time.Now()
}

// execCommand is a variable to allow mocking exec.Command in tests. It is used by openBrowser to launch the default web browser.
var execCommand = exec.Command //nolint:gochecknoglobals // test seam for mocking exec.Command in unit tests

// openBrowser opens the specified URL in the default browser.
func openBrowser(url string) {
	var cmd *exec.Cmd

	switch runtime.GOOS {
	case "windows":
		cmd = execCommand("cmd", "/c", "start", url)
	case "darwin":
		cmd = execCommand("open", url)
	default: // linux and others
		cmd = execCommand("xdg-open", url)
	}

	if err := cmd.Start(); err != nil {
		logger.Sugar.Warnf("Failed to open browser: %v", err)
		return
	}
	logger.Sugar.Infof("Opened health page in browser: %s", url)
}

// SignMessages signs a registration message using both DefraDB and P2P keys.
func (i *ChainIndexer) SignMessages(message string) (server.DefraPKRegistration, server.PeerIDRegistration, error) {
	signedMsg, err := signer.SignWithDefraKeys(message, i.defraNode, i.cfg)
	if err != nil {
		return server.DefraPKRegistration{}, server.PeerIDRegistration{}, err
	}

	// Sign with peer ID
	peerSignedMsg, err := signer.SignWithP2PKeys(message, i.defraNode, i.cfg)
	if err != nil {
		return server.DefraPKRegistration{}, server.PeerIDRegistration{}, err
	}

	// Get node and peer public keys from signer helpers.
	nodePubKey, err := i.GetNodePublicKey()
	if err != nil {
		return server.DefraPKRegistration{}, server.PeerIDRegistration{}, fmt.Errorf("failed to get node public key: %w", err)
	}

	peerPubKey, err := i.GetPeerPublicKey()
	if err != nil {
		return server.DefraPKRegistration{}, server.PeerIDRegistration{}, fmt.Errorf("failed to get peer public key: %w", err)
	}

	return server.DefraPKRegistration{
			PublicKey:   nodePubKey,
			SignedPKMsg: signedMsg,
		}, server.PeerIDRegistration{
			PeerID:        peerPubKey,
			SignedPeerMsg: peerSignedMsg,
		}, nil
}

// SignRegistrationMessage signs a registration message using only the DefraDB identity key.
func (i *ChainIndexer) SignRegistrationMessage(message string) (server.DefraPKRegistration, error) {
	signedMsg, err := signer.SignWithDefraKeys(message, i.defraNode, i.cfg)
	if err != nil {
		return server.DefraPKRegistration{}, err
	}

	nodePubKey, err := i.GetNodePublicKey()
	if err != nil {
		return server.DefraPKRegistration{}, fmt.Errorf("failed to get node public key: %w", err)
	}

	return server.DefraPKRegistration{
		PublicKey:   nodePubKey,
		SignedPKMsg: signedMsg,
	}, nil
}

// GetSourceChainInfo returns the ShinzoHub registration source chain metadata for this indexer.
func (i *ChainIndexer) GetSourceChainInfo() (string, uint64) {
	if i == nil || i.cfg == nil {
		return "", 0
	}

	name := strings.ToLower(strings.TrimSpace(i.cfg.Chain.Name))
	network := strings.ToLower(strings.TrimSpace(i.cfg.Chain.Network))
	if (name == "" || name == "ethereum") && (network == "" || network == "mainnet") {
		return "ethereum", 1
	}

	return "", 0
}

// GetNodePublicKey returns the DefraDB node's public key as a hex string.
func (i *ChainIndexer) GetNodePublicKey() (string, error) {
	return signer.GetDefraPublicKey(i.defraNode, i.cfg)
}

// GetPeerPublicKey returns the P2P peer's public key as a hex string.
func (i *ChainIndexer) GetPeerPublicKey() (string, error) {
	return signer.GetP2PPublicKey(i.defraNode, i.cfg)
}

// GetPrunerMetrics returns the current pruner metrics, or nil if pruner is not enabled.
func (i *ChainIndexer) GetPrunerMetrics() *pruner.Metrics {
	if i.pruner == nil {
		return nil
	}
	metrics := i.pruner.GetMetrics()
	return &metrics
}

// newAuthenticator constructs an Authenticator based on the configured auth mode.
func newAuthenticator(mode string, keys []string) (server.Authenticator, error) {
	switch mode {
	case constants.SchemaAuthModeNone:
		return server.NoOpAuthenticator{}, nil
	case constants.SchemaAuthModeToken, "":
		return server.NewBearerAuthenticator(keys), nil
	case constants.SchemaAuthModeMTLS:
		return nil, ErrMTLSNotImplemented
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownAuthMode, mode)
	}
}

// indexerQueueTracker adapts pruner's IndexerQueue to the local DocIDTrackerInterface.
type indexerQueueTracker struct {
	queue       *pruner.IndexerQueue
	collections *constants.CollectionNames
}

func (t *indexerQueueTracker) TrackBlock(_ context.Context, blockNumber int64, result *defra.BlockCreationResult) error {
	otherDocIDs := map[string][]string{
		t.collections.Transaction:     result.TransactionIDs,
		t.collections.Log:             result.LogIDs,
		t.collections.AccessListEntry: result.AccessListIDs,
	}
	return t.queue.TrackBlockDocIDs(blockNumber, result.BlockID, otherDocIDs, result.BlockSignatureID)
}
