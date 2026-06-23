package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "github.com/ava-labs/simplex/jepsen/node/proto"
)

var tests = []struct {
	name string
	fn   func(*Cluster) error
}{
	{"safety", testSafety},
}

type Cluster struct {
	dir 	string 
	procs 	[]*os.Process
	addrs 	[]string
	nodeIDs []string
	ctrl    []pb.ControlServiceClient
	admin   []pb.AdminServiceClient
}

func main() {
	testName := flag.String("test", "", "run a specific test by name (default: run all)")
	flag.Parse()

	cluster, err := setupCluster(4)
	if err != nil {
		fmt.Fprintf(os.Stderr, "setup failed: %v\n", err)
		os.Exit(1)
	}
	defer cluster.TearDown()

	failed := false
	for _, t := range tests {
		if *testName != "" && t.name != *testName {
			continue
		}
		if err := t.fn(cluster); err != nil {
			fmt.Fprintf(os.Stderr, "FAIL %s: %v\n", t.name, err)
			failed = true
		} else {
			fmt.Printf("PASS %s\n", t.name)
		}
	}
	if failed {
		os.Exit(1)
	}
}

// Setup a cluster of n nodes, returning a Cluster struct with the node processes and gRPC clients.
func setupCluster(n int) (*Cluster, error) {
	// 1. Generate configs for 4 nodes.
	dir, err := os.MkdirTemp("", "simplex-smoke-*")
	if err != nil {
		return &Cluster{},err
	}
	
	// Determine the module root (jepsen/node directory).
	moduleRoot, err := filepath.Abs(filepath.Join("."))
	if err != nil {
		return &Cluster{},err
	}
	// When run via `go run`, working dir is the package dir.
	// Walk up to find go.mod for jepsen/node.
	for {
		if _, err := os.Stat(filepath.Join(moduleRoot, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(moduleRoot)
		if parent == moduleRoot {
			return &Cluster{},fmt.Errorf("could not find go.mod")
		}
		moduleRoot = parent
	}

	genconfigBin := filepath.Join(dir, "simplex-genconfig")
	buildGenconfig := exec.Command("go", "build", "-o", genconfigBin, "./scripts/genconfig/")
	buildGenconfig.Dir = moduleRoot
	if out, err := buildGenconfig.CombinedOutput(); err != nil {
		return &Cluster{},fmt.Errorf("build genconfig: %s: %w", out, err)
	}

	nodeBin := filepath.Join(dir, "simplex-node")
	buildNode := exec.Command("go", "build", "-o", nodeBin, "./cmd/")
	buildNode.Dir = moduleRoot
	if out, err := buildNode.CombinedOutput(); err != nil {
		return &Cluster{},fmt.Errorf("build node: %s: %w", out, err)
	}

	basePort, err := findFreePort()
	if err != nil {
		return nil, fmt.Errorf("find free port: %w", err)
	}

	configDir := filepath.Join(dir, "configs")
	genCmd := exec.Command(genconfigBin, "-n", fmt.Sprintf("%d", n), "-out", configDir, "-base-port", fmt.Sprintf("%d", basePort))
	if out, err := genCmd.CombinedOutput(); err != nil {
		return &Cluster{},fmt.Errorf("genconfig: %s: %w", out, err)
	}

	// 2. Start all n nodes.
	cmds := make([]*exec.Cmd, n)
	for i := 0; i < n; i++ {
		cfgPath := filepath.Join(configDir, fmt.Sprintf("node%d", i), "config.json")
		c := exec.Command(nodeBin, "-config", cfgPath)
		c.Stdout = os.Stdout
		c.Stderr = os.Stderr
		if err := c.Start(); err != nil {
			return &Cluster{},fmt.Errorf("start node%d: %w", i, err)
		}
		cmds[i] = c
	}

	// 3. Wait for all nodes to be ready (poll GetStatus).
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
	}
	if err := waitForNodes(ctx, addrs); err != nil {
		return &Cluster{},fmt.Errorf("nodes did not become ready: %w", err)
	}
	fmt.Println("all nodes ready")

	// 4. Dial all nodes 
	ctrlClients := make([]pb.ControlServiceClient, n)
	adminClients := make([]pb.AdminServiceClient, n)
	for i:=0; i< n; i++ {
		conn, err := grpc.NewClient(addrs[i], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("dial node%d: %w", i, err)
		}
		ctrlClients[i] = pb.NewControlServiceClient(conn)
		adminClients[i] = pb.NewAdminServiceClient(conn)
	}

	// 5. read all NodeIDs from config files 
	addrToNodeID := make (map[string] string)
	for i := range n {
		cfgPath := filepath.Join(configDir, fmt.Sprintf("node%d", i), "config.json")
		data, err := os.ReadFile(cfgPath)
		if err != nil {
			return nil, fmt.Errorf ("read config node%d: %w", i, err)
		}
		var cfg struct {
			Peers []struct {
				NodeID  string `json:"node_id"`
				Addr 	string `json:"addr"`
			} `json:"peers"`
		}
		if err := json.Unmarshal(data, &cfg); err != nil {
        return nil, fmt.Errorf("parse config node%d: %w", i, err)
		}
		for _, p := range cfg.Peers {
			addrToNodeID[p.Addr] = p.NodeID
		}
	}
	nodeIDs := make([]string, n)
	for i := range n {
		nodeIDs[i] = addrToNodeID[fmt.Sprintf("127.0.0.1:%d", basePort+i)]
	}
	return &Cluster{
		dir:    dir,
		procs: func() []*os.Process{
			p:= make([]*os.Process, n)
			for i, c := range cmds {p[i] = c.Process}
			return p
		}(),
		addrs: addrs,
		nodeIDs: nodeIDs,
		ctrl: ctrlClients,
		admin: adminClients,
	}, nil

}
func findFreePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port, nil
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

func (c *Cluster) TearDown(){
	for _, p := range c.procs {
		p.Kill()
	}
	os.RemoveAll(c.dir)

}

func (c *Cluster) waitForBlocks (minSeq uint64, timepout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timepout)
	defer cancel()
	for {
		allReady := true
		for i, ctrl := range c.ctrl {
			resp, err := ctrl.GetStatus(ctx, &pb.GetStatusRequest{})
			if err != nil || resp.Seq < minSeq {
				fmt.Printf("node %d (%s) lagging: seq=%d, want=%d\n", i, c.addrs[i], resp.GetSeq(), minSeq)
				allReady = false 
				break
			}
		}
		if allReady {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for all nodes to reach seq %d", minSeq)
		case <-time.After(200 * time.Millisecond):
		}	
	}	
}


func (c *Cluster) verifyAgreement() error {
	ctx := context.Background()
	type blockKey struct {seq, round, epoch uint64}
	digests := make(map[blockKey]string)

	for i, ctrl := range c.ctrl {
			resp, err := ctrl.GetCommittedBlocks(ctx, &pb.GetCommittedBlocksRequest{FromSeq: 0})
			if err != nil {
				return fmt.Errorf("GetCommittedBlocks from node %d (%s): %w", i, c.addrs[i], err)
			}
			for _, b := range resp.Blocks {
				k := blockKey{b.Seq, b.Round, b.Epoch}
				dgst := fmt.Sprintf("%x", b.Digest)
				if existing, ok := digests[k]; ok {
					if existing != dgst {
						return fmt.Errorf("SAFETY VIOLATION: node %d (%s) disagrees on seq=%d: got %s, expected %s",
                        i, c.addrs[i], b.Seq, dgst, existing)
					}
				} else {
					digests[k] = dgst
				}
			}
		}
	fmt.Printf("agreement verified across %d nodes, %d blocks\n", len(c.ctrl), len(digests))
	return nil
}


func (c *Cluster) Partition (nodeIdx int, peerIdxs ...int) error {
	peerIDs := make ([][]byte, len(peerIdxs))
	for i, idx := range peerIdxs {
		b, err := hex.DecodeString(c.nodeIDs[idx])
		if err != nil {
            return fmt.Errorf("decode nodeID for node%d: %w", idx, err)
        }
		peerIDs[i] = b
	}
	_, err := c.admin[nodeIdx].Partition(context.Background(), &pb.PartitionRequest{PeerIds: peerIDs})
    return err
}

func (c *Cluster) Heal(nodeIdx int) error {
    _, err := c.admin[nodeIdx].Heal(context.Background(), &pb.HealRequest{})
    return err
}



// Tests 

func testSafety (c *Cluster) error {
	ctx := context.Background()
	_, err := c.ctrl[0].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx1")})
	if err != nil {
		return fmt.Errorf("SubmitTransaction: %w", err)
	}
	if err := c.waitForBlocks(1, 60*time.Second); err != nil {
        return err
    }
	return c.verifyAgreement()
}


