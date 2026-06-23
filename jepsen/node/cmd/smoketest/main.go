// smoketest starts a 4-node cluster, submits a transaction, and verifies
// that all nodes commit the same block.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/ava-labs/simplex/jepsen/node/proto"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "smoketest:", err)
		os.Exit(1)
	}
	fmt.Println("PASS")
}

func run() error {
	// 1. Generate configs for 4 nodes.
	dir, err := os.MkdirTemp("", "simplex-smoke-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	// Determine the module root (jepsen/node directory).
	moduleRoot, err := filepath.Abs(filepath.Join("."))
	if err != nil {
		return err
	}
	// When run via `go run`, working dir is the package dir.
	// Walk up to find go.mod for jepsen/node.
	for {
		if _, err := os.Stat(filepath.Join(moduleRoot, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(moduleRoot)
		if parent == moduleRoot {
			return fmt.Errorf("could not find go.mod")
		}
		moduleRoot = parent
	}

	genconfigBin := filepath.Join(dir, "simplex-genconfig")
	buildGenconfig := exec.Command("go", "build", "-o", genconfigBin, "./scripts/genconfig/")
	buildGenconfig.Dir = moduleRoot
	if out, err := buildGenconfig.CombinedOutput(); err != nil {
		return fmt.Errorf("build genconfig: %s: %w", out, err)
	}

	nodeBin := filepath.Join(dir, "simplex-node")
	buildNode := exec.Command("go", "build", "-o", nodeBin, "./cmd/")
	buildNode.Dir = moduleRoot
	if out, err := buildNode.CombinedOutput(); err != nil {
		return fmt.Errorf("build node: %s: %w", out, err)
	}

	configDir := filepath.Join(dir, "configs")
	genCmd := exec.Command(genconfigBin, "-n", "4", "-out", configDir, "-base-port", "19000")
	if out, err := genCmd.CombinedOutput(); err != nil {
		return fmt.Errorf("genconfig: %s: %w", out, err)
	}

	// 2. Start all 4 nodes.
	cmds := make([]*exec.Cmd, 4)
	for i := 0; i < 4; i++ {
		cfgPath := filepath.Join(configDir, fmt.Sprintf("node%d", i), "config.json")
		c := exec.Command(nodeBin, "-config", cfgPath)
		c.Stdout = os.Stdout
		c.Stderr = os.Stderr
		if err := c.Start(); err != nil {
			return fmt.Errorf("start node%d: %w", i, err)
		}
		cmds[i] = c
	}
	defer func() {
		for _, c := range cmds {
			c.Process.Kill()
		}
	}()
	// 3. Wait for all nodes to be ready (poll GetStatus).
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	addrs := []string{"127.0.0.1:19000", "127.0.0.1:19001", "127.0.0.1:19002", "127.0.0.1:19003"}
	if err := waitForNodes(ctx, addrs); err != nil {
		return fmt.Errorf("nodes did not become ready: %w", err)
	}
	fmt.Println("all nodes ready")

	// 4. Dial node0's ControlService and submit a transaction.
	conn, err := grpc.NewClient("127.0.0.1:19000",
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial node0: %w", err)
	}
	defer conn.Close()
	ctrl := pb.NewControlServiceClient(conn)

	_, err = ctrl.SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("hello simplex")})
	if err != nil {
		return fmt.Errorf("SubmitTransaction: %w", err)
	}
	fmt.Println("transaction submitted")

	// 5. Wait for at least 1 committed block on all nodes.
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		allCommitted := true
		for _, addr := range addrs {
			conn2, _ := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			ctrl2 := pb.NewControlServiceClient(conn2)
			resp, err := ctrl2.GetStatus(ctx, &pb.GetStatusRequest{})
			conn2.Close()
			if err != nil || resp.Seq == 0 {
				allCommitted = false
				break
			}
		}
		if allCommitted {
			fmt.Println("all nodes committed at least 1 block")
			// 6. Verify they agree on the same block.
			return verifyAgreement(ctx, addrs)
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for blocks to commit")
}

// waitForNodes polls all nodes until they all respond to GetStatus.
func waitForNodes(ctx context.Context, addrs []string) error {
	for {
		allReady := true
		for _, addr := range addrs {
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				allReady = false
				conn.Close()
				break
			}
			ctrl := pb.NewControlServiceClient(conn)
			_, err = ctrl.GetStatus(ctx, &pb.GetStatusRequest{})
			conn.Close()
			if err != nil {
				allReady = false
				break
			}
		}
		if allReady {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func verifyAgreement(ctx context.Context, addrs []string) error {
	type blockKey struct{ seq, round, epoch uint64 }
	digests := make(map[blockKey]string) // seq → hex digest (from first node that has it)

	for _, addr := range addrs {
		conn, _ := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		ctrl := pb.NewControlServiceClient(conn)
		resp, err := ctrl.GetCommittedBlocks(ctx, &pb.GetCommittedBlocksRequest{FromSeq: 0})
		conn.Close()
		if err != nil {
			return fmt.Errorf("GetCommittedBlocks from %s: %w", addr, err)
		}
		for _, b := range resp.Blocks {
			k := blockKey{b.Seq, b.Round, b.Epoch}
			dgst := fmt.Sprintf("%x", b.Digest)
			if existing, ok := digests[k]; ok {
				if existing != dgst {
					return fmt.Errorf("SAFETY VIOLATION: node %s disagrees on block seq=%d: got %s, expected %s",
						addr, b.Seq, dgst, existing)
				}
			} else {
				digests[k] = dgst
			}
		}
		out, _ := json.MarshalIndent(resp.Blocks, "", "  ")
		fmt.Printf("node %s committed blocks: %s\n", addr, out)
	}
	fmt.Printf("agreement verified across %d nodes, %d blocks\n", len(addrs), len(digests))
	return nil
}
