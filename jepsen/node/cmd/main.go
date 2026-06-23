// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"syscall"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"

	"github.com/ava-labs/simplex/common"
	simplexengine "github.com/ava-labs/simplex/simplex"
	"github.com/ava-labs/simplex/wal"

	"github.com/ava-labs/simplex/jepsen/node/internal/block"
	"github.com/ava-labs/simplex/jepsen/node/internal/comm"
	"github.com/ava-labs/simplex/jepsen/node/internal/crypto"
	"github.com/ava-labs/simplex/jepsen/node/internal/storage"
	pb "github.com/ava-labs/simplex/jepsen/node/proto"
)

type Config struct {
	ListenAddr         string       `json:"listen_addr"`
	DataDir            string       `json:"data_dir"`
	KeyFile            string       `json:"key_file"`
	Peers              []PeerConfig `json:"peers"`
	MaxProposalWaitMs  int64        `json:"max_proposal_wait_ms"`
	ReplicationEnabled bool         `json:"replication_enabled"`
}

type PeerConfig struct {
	// NodeID is the hex-encoded compressed BLS public key of the peer.
	NodeID string `json:"node_id"`
	// Addr is the "host:port" gRPC address.
	Addr string `json:"addr"`
}

func main() {
	configPath := flag.String("config", "config.json", "path to JSON config file")
	debug := flag.Bool("debug", false, "enable debug logging")
	flag.Parse()
	if *debug {
		os.Setenv("SIMPLEX_DEBUG", "1")
	}

	cfg, err := loadConfig(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	if err := run(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "node error: %v\n", err)
		os.Exit(1)
	}
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	return &cfg, nil
}

// zapLogger wraps a *zap.Logger to satisfy common.Logger.
// Trace and Verbo are mapped to Debug level.
type zapLogger struct {
	l *zap.Logger
}

func (z *zapLogger) Fatal(msg string, fields ...zap.Field) { z.l.Fatal(msg, fields...) }
func (z *zapLogger) Error(msg string, fields ...zap.Field) { z.l.Error(msg, fields...) }
func (z *zapLogger) Warn(msg string, fields ...zap.Field)  { z.l.Warn(msg, fields...) }
func (z *zapLogger) Info(msg string, fields ...zap.Field)  { z.l.Info(msg, fields...) }
func (z *zapLogger) Trace(msg string, fields ...zap.Field) { z.l.Debug(msg, fields...) }
func (z *zapLogger) Debug(msg string, fields ...zap.Field) { z.l.Debug(msg, fields...) }
func (z *zapLogger) Verbo(msg string, fields ...zap.Field) { z.l.Debug(msg, fields...) }

func run(cfg *Config) error {
	// 1. Set up logger.
	zapCfg := zap.NewProductionConfig()
	zapCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	if os.Getenv("SIMPLEX_DEBUG") == "1" {
		zapCfg.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	}
	logger, err := zapCfg.Build()
	if err != nil {
		return fmt.Errorf("build logger: %w", err)
	}
	defer logger.Sync()
	log := &zapLogger{l: logger}

	// 2. Load or generate BLS key.
	signer, err := loadOrGenerateKey(cfg.KeyFile)
	if err != nil {
		return fmt.Errorf("load key: %w", err)
	}
	selfID := signer.NodeID()
	logger.Info("node identity", zap.String("nodeID", fmt.Sprintf("%x", []byte(selfID))))

	// 3. Build node list and peer address map.
	nodes, peerAddrs, err := buildNodeList(cfg, selfID)
	if err != nil {
		return fmt.Errorf("build node list: %w", err)
	}

	// 4. Create crypto components.
	verifier := crypto.NewVerifier(nodes)
	aggregator := crypto.NewAggregator(nodes)
	qcDeserializer := crypto.NewQCDeserializer(nodes)

	// SignatureAggregatorCreator satisfies common.SignatureAggregatorCreator.
	sigAggCreator := func(nodeList []common.Node) common.SignatureAggregator {
		return crypto.NewAggregator(nodeList)
	}
	_ = aggregator // used via sigAggCreator

	// 5. Create block deserializer and builder.
	bd := &block.BlockDeserializer{}
	bb := block.NewBlockBuilder()

	// 6. Create storage.
	dataDir := cfg.DataDir
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("mkdir data_dir: %w", err)
	}
	stor, err := storage.New(dataDir, bd, qcDeserializer)
	if err != nil {
		return fmt.Errorf("create storage: %w", err)
	}
	defer stor.Close()

	// 7. Open WAL.
	walPath := filepath.Join(dataDir, "wal.log")
	w := wal.New(walPath)
	defer w.Close()

	// 8. Shared epoch status (updated once epoch is created).
	var epochRef *simplexengine.Epoch

	statusFunc := func() (uint64, uint64, uint64) {
		// epoch=0 (single epoch for testing), round and seq from storage
		return 0, 0, stor.NumBlocks()
	}

	// 9. Create Comm (gRPC).
	commInst, err := comm.New(
		selfID,
		nodes,
		peerAddrs,
		// MessageHandler: will be set after epoch is created.
		// Use a deferred handler that forwards to epochRef.
		&deferredHandler{ref: &epochRef},
		bb,
		statusFunc,
		stor,
		bd,
		qcDeserializer,
		logger,
	)
	if err != nil {
		return fmt.Errorf("create comm: %w", err)
	}

	// 10. Configure epoch.
	maxProposalWait := time.Duration(cfg.MaxProposalWaitMs) * time.Millisecond
	if maxProposalWait == 0 {
		maxProposalWait = simplexengine.DefaultMaxProposalWaitTime
	}

	epochCfg := simplexengine.EpochConfig{
		MaxProposalWait:            maxProposalWait,
		MaxRoundWindow:             simplexengine.DefaultMaxRoundWindow,
		MaxRebroadcastWait:         simplexengine.DefaultEmptyVoteRebroadcastTimeout,
		FinalizeRebroadcastTimeout: simplexengine.DefaultFinalizeVoteRebroadcastTimeout,
		QCDeserializer:             qcDeserializer,
		Logger:                     log,
		ID:                         selfID,
		Signer:                     signer,
		Verifier:                   verifier,
		BlockDeserializer:          bd,
		SignatureAggregatorCreator: sigAggCreator,
		Comm:                       commInst,
		Storage:                    stor,
		WAL:                        w,
		BlockBuilder:               bb,
		Epoch:                      0,
		StartTime:                  time.Now(),
		ReplicationEnabled:         cfg.ReplicationEnabled,
		RandomSource:               crypto.NewRandomSource(),
	}

	epoch, err := simplexengine.NewEpoch(epochCfg)
	if err != nil {
		return fmt.Errorf("create epoch: %w", err)
	}
	epochRef = epoch

	// 11. Start the gRPC server.
	lis, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.ListenAddr, err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterNodeServiceServer(grpcServer, commInst)
	pb.RegisterControlServiceServer(grpcServer, commInst)

	go func() {
		logger.Info("gRPC server listening", zap.String("addr", cfg.ListenAddr))
		if err := grpcServer.Serve(lis); err != nil {
			logger.Error("gRPC server error", zap.Error(err))
		}
	}()

	// 12. Start time-advance ticker (50ms).
	stopTicker := make(chan struct{})
	go func() {
		ticker := time.NewTicker(50 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case t := <-ticker.C:
				epoch.AdvanceTime(t)
			case <-stopTicker:
				return
			}
		}
	}()

	// 13. Start the consensus engine.
	if err := epoch.Start(); err != nil {
		return fmt.Errorf("epoch.Start: %w", err)
	}
	logger.Info("Simplex epoch started")

	// 14. Wait for shutdown signal.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down")
	grpcServer.GracefulStop()
	close(stopTicker)
	return nil
}

// deferredHandler holds a pointer-to-pointer to Epoch so it can be set after
// Comm is created but before the engine is started.
type deferredHandler struct {
	ref **simplexengine.Epoch
}

func (d *deferredHandler) HandleMessage(msg *common.Message, from common.NodeID) error {
	ep := *d.ref
	if ep == nil {
		return fmt.Errorf("epoch not yet initialized")
	}
	return ep.HandleMessage(msg, from)
}

// loadOrGenerateKey loads a BLS secret key from keyFile, or generates and saves one.
func loadOrGenerateKey(keyFile string) (*crypto.Signer, error) {
	data, err := os.ReadFile(keyFile)
	if err == nil {
		skBytes, err := hex.DecodeString(string(data))
		if err != nil {
			return nil, fmt.Errorf("decode key hex: %w", err)
		}
		return crypto.NewSignerFromBytes(skBytes)
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read key file: %w", err)
	}

	// Generate fresh key.
	s, err := crypto.NewSigner()
	if err != nil {
		return nil, err
	}
	encoded := hex.EncodeToString(s.SecretKeyBytes())
	if err := os.WriteFile(keyFile, []byte(encoded), 0600); err != nil {
		return nil, fmt.Errorf("write key file: %w", err)
	}
	return s, nil
}

// buildNodeList builds common.Nodes from peers in the config, including self.
// Nodes are sorted by NodeID so all participants agree on the same canonical order,
// which is required for consistent leader election across nodes.
func buildNodeList(cfg *Config, selfID common.NodeID) (common.Nodes, map[string]string, error) {
	peerAddrs := make(map[string]string, len(cfg.Peers))
	var nodes common.Nodes

	// Include self.
	nodes = append(nodes, common.Node{Node: selfID, Weight: 1})

	for _, p := range cfg.Peers {
		nodeIDBytes, err := hex.DecodeString(p.NodeID)
		if err != nil {
			return nil, nil, fmt.Errorf("decode peer nodeID %q: %w", p.NodeID, err)
		}
		nodeID := common.NodeID(nodeIDBytes)
		if nodeID.Equals(selfID) {
			peerAddrs[p.NodeID] = p.Addr
			continue
		}
		nodes = append(nodes, common.Node{Node: nodeID, Weight: 1})
		peerAddrs[p.NodeID] = p.Addr
	}

	// Sort by NodeID bytes so all nodes agree on the same canonical order.
	sort.Slice(nodes, func(i, j int) bool {
		return bytes.Compare(nodes[i].Node, nodes[j].Node) < 0
	})

	return nodes, peerAddrs, nil
}
