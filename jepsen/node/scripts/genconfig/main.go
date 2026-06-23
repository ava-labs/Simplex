// genconfig generates BLS keys and config files for a local multi-node test.
//
// Usage:
//
//	go run ./scripts/genconfig -n 4 -out ./testconfig
//
// This creates testconfig/node0/, testconfig/node1/, ... each containing:
//   - key.hex       — hex-encoded BLS secret key
//   - config.json   — node configuration (peers include all other nodes)
package main

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ava-labs/simplex/jepsen/node/internal/crypto"
)

// nodeConfig mirrors the Config struct in cmd/main.go.
type nodeConfig struct {
	ListenAddr         string       `json:"listen_addr"`
	DataDir            string       `json:"data_dir"`
	KeyFile            string       `json:"key_file"`
	Peers              []peerConfig `json:"peers"`
	MaxProposalWaitMs  int64        `json:"max_proposal_wait_ms"`
	ReplicationEnabled bool         `json:"replication_enabled"`
}

type peerConfig struct {
	NodeID string `json:"node_id"`
	Addr   string `json:"addr"`
}

func main() {
	n := flag.Int("n", 4, "number of nodes to generate configs for")
	outDir := flag.String("out", "./testconfig", "output directory")
	basePort := flag.Int("base-port", 9000, "first gRPC port; subsequent nodes use base+1, base+2, ...")
	flag.Parse()

	if err := run(*n, *outDir, *basePort); err != nil {
		fmt.Fprintf(os.Stderr, "genconfig error: %v\n", err)
		os.Exit(1)
	}
}

func run(n int, outDir string, basePort int) error {
	// Generate a key and address for each node.
	type nodeInfo struct {
		signer *crypto.Signer
		nodeID string // hex
		addr   string // host:port
	}

	infos := make([]nodeInfo, n)
	for i := 0; i < n; i++ {
		s, err := crypto.NewSigner()
		if err != nil {
			return fmt.Errorf("generate key for node %d: %w", i, err)
		}
		infos[i] = nodeInfo{
			signer: s,
			nodeID: hex.EncodeToString(s.NodeID()),
			addr:   fmt.Sprintf("127.0.0.1:%d", basePort+i),
		}
	}

	// Write per-node directories.
	for i, info := range infos {
		dir := filepath.Join(outDir, fmt.Sprintf("node%d", i))
		dataDir := filepath.Join(dir, "data")
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			return fmt.Errorf("mkdir node%d: %w", i, err)
		}

		// Write key file.
		keyFile := filepath.Join(dir, "key.hex")
		skHex := hex.EncodeToString(info.signer.SecretKeyBytes())
		if err := os.WriteFile(keyFile, []byte(skHex), 0600); err != nil {
			return fmt.Errorf("write key for node%d: %w", i, err)
		}

		// Build peer list (everyone else).
		peers := make([]peerConfig, 0, n)
		for j, peer := range infos {
			if j == i {
				continue
			}
			peers = append(peers, peerConfig{
				NodeID: peer.nodeID,
				Addr:   peer.addr,
			})
		}

		cfg := nodeConfig{
			ListenAddr:         info.addr,
			DataDir:            dataDir,
			KeyFile:            keyFile,
			Peers:              peers,
			MaxProposalWaitMs:  2000,
			ReplicationEnabled: true,
		}

		cfgBytes, err := json.MarshalIndent(cfg, "", "  ")
		if err != nil {
			return fmt.Errorf("marshal config for node%d: %w", i, err)
		}
		cfgFile := filepath.Join(dir, "config.json")
		if err := os.WriteFile(cfgFile, cfgBytes, 0644); err != nil {
			return fmt.Errorf("write config for node%d: %w", i, err)
		}

		fmt.Printf("node%d: nodeID=%s addr=%s config=%s\n", i, info.nodeID, info.addr, cfgFile)
	}

	return nil
}
