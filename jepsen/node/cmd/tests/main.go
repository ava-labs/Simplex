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
	"sort"
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
	{"partition", testPartition},
	{"partial-partition", testPartialPartition},
	{"netsplit", testNetSplit},
	{"repartition", testRePartition},
	{"rolling-partition", testRollingPartition},
	{"cascading-partition", testCascadingPartition},
	{"cross-minority-partition", testCrossMinorityPartition},
	{"leader-assassination", testLeaderAssassination},
	{"flapping-partition", testFlappingPartition},
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

	bins, err := buildBinaries()
	if err != nil {
		fmt.Fprintf(os.Stderr, "build failed: %v\n", err)
		os.Exit(1)
	}
	defer bins.Cleanup()

	failed := false
	for _, t := range tests {
		if *testName != "" && t.name != *testName {
			continue
		}
		cluster, err := setupCluster(4, bins)
		if err != nil {
			fmt.Fprintf(os.Stderr, "FAIL %s (setup): %v\n", t.name, err)
			failed = true
			continue
		}
		if err := t.fn(cluster); err != nil {
			fmt.Fprintf(os.Stderr, "FAIL %s: %v\n", t.name, err)
			failed = true
		} else {
			fmt.Printf("PASS %s\n", t.name)
		}
		cluster.TearDown()
	}
	if failed {
		os.Exit(1)
	}
}

// Binaries holds pre-built binary paths shared across all test cluster setups.
type Binaries struct {
	dir          string
	genconfigBin string
	nodeBin      string
}

func buildBinaries() (*Binaries, error) {
	moduleRoot, err := filepath.Abs(".")
	if err != nil {
		return nil, err
	}
	for {
		if _, err := os.Stat(filepath.Join(moduleRoot, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(moduleRoot)
		if parent == moduleRoot {
			return nil, fmt.Errorf("could not find go.mod")
		}
		moduleRoot = parent
	}

	dir, err := os.MkdirTemp("", "simplex-bins-*")
	if err != nil {
		return nil, err
	}

	genconfigBin := filepath.Join(dir, "simplex-genconfig")
	cmd := exec.Command("go", "build", "-o", genconfigBin, "./scripts/genconfig/")
	cmd.Dir = moduleRoot
	if out, err := cmd.CombinedOutput(); err != nil {
		os.RemoveAll(dir)
		return nil, fmt.Errorf("build genconfig: %s: %w", out, err)
	}

	nodeBin := filepath.Join(dir, "simplex-node")
	cmd = exec.Command("go", "build", "-o", nodeBin, "./cmd/")
	cmd.Dir = moduleRoot
	if out, err := cmd.CombinedOutput(); err != nil {
		os.RemoveAll(dir)
		return nil, fmt.Errorf("build node: %s: %w", out, err)
	}

	return &Binaries{dir: dir, genconfigBin: genconfigBin, nodeBin: nodeBin}, nil
}

func (b *Binaries) Cleanup() {
	os.RemoveAll(b.dir)
}

// setupCluster starts n fresh nodes using pre-built binaries and returns a ready Cluster.
func setupCluster(n int, bins *Binaries) (*Cluster, error) {
	dir, err := os.MkdirTemp("", "simplex-cluster-*")
	if err != nil {
		return nil, err
	}

	basePort, err := findFreePort()
	if err != nil {
		os.RemoveAll(dir)
		return nil, fmt.Errorf("find free port: %w", err)
	}

	configDir := filepath.Join(dir, "configs")
	genCmd := exec.Command(bins.genconfigBin, "-n", fmt.Sprintf("%d", n), "-out", configDir, "-base-port", fmt.Sprintf("%d", basePort))
	if out, err := genCmd.CombinedOutput(); err != nil {
		os.RemoveAll(dir)
		return nil, fmt.Errorf("genconfig: %s: %w", out, err)
	}

	cmds := make([]*exec.Cmd, n)
	for i := 0; i < n; i++ {
		cfgPath := filepath.Join(configDir, fmt.Sprintf("node%d", i), "config.json")
		c := exec.Command(bins.nodeBin, "-config", cfgPath)
		c.Stdout = os.Stdout
		c.Stderr = os.Stderr
		if err := c.Start(); err != nil {
			for j := 0; j < i; j++ {
				cmds[j].Process.Kill()
			}
			os.RemoveAll(dir)
			return nil, fmt.Errorf("start node%d: %w", i, err)
		}
		cmds[i] = c
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", basePort+i)
	}
	if err := waitForNodes(ctx, addrs); err != nil {
		for _, c := range cmds {
			c.Process.Kill()
		}
		os.RemoveAll(dir)
		return nil, fmt.Errorf("nodes did not become ready: %w", err)
	}
	fmt.Println("all nodes ready")

	ctrlClients := make([]pb.ControlServiceClient, n)
	adminClients := make([]pb.AdminServiceClient, n)
	for i := 0; i < n; i++ {
		conn, err := grpc.NewClient(addrs[i], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			for _, c := range cmds {
				c.Process.Kill()
			}
			os.RemoveAll(dir)
			return nil, fmt.Errorf("dial node%d: %w", i, err)
		}
		ctrlClients[i] = pb.NewControlServiceClient(conn)
		adminClients[i] = pb.NewAdminServiceClient(conn)
	}

	addrToNodeID := make(map[string]string)
	for i := range n {
		cfgPath := filepath.Join(configDir, fmt.Sprintf("node%d", i), "config.json")
		data, err := os.ReadFile(cfgPath)
		if err != nil {
			return nil, fmt.Errorf("read config node%d: %w", i, err)
		}
		var cfg struct {
			Peers []struct {
				NodeID string `json:"node_id"`
				Addr   string `json:"addr"`
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

	procs := make([]*os.Process, n)
	for i, c := range cmds {
		procs[i] = c.Process
	}

	return &Cluster{
		dir:     dir,
		procs:   procs,
		addrs:   addrs,
		nodeIDs: nodeIDs,
		ctrl:    ctrlClients,
		admin:   adminClients,
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

func (c *Cluster) waitForBlocks(minSeq uint64, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	tick := 0
	for {
		allReady := true
		for i, ctrl := range c.ctrl {
			resp, err := ctrl.GetStatus(ctx, &pb.GetStatusRequest{})
			if err != nil || resp.Seq < minSeq {
				if tick%10 == 0 {
					fmt.Printf("node %d (%s) lagging: seq=%d, want=%d\n", i, c.addrs[i], resp.GetSeq(), minSeq)
				}
				allReady = false
				break
			}
		}
		if allReady {
			return nil
		}
		tick++
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


func (c *Cluster) Partition(nodeIdx int, peerIdxs ...int) error {
	peerIDs := make([][]byte, len(peerIdxs))
	for i, idx := range peerIdxs {
		b, err := hex.DecodeString(c.nodeIDs[idx])
		if err != nil {
			return fmt.Errorf("decode nodeID for node%d: %w", idx, err)
		}
		peerIDs[i] = b
	}
	_, err := c.admin[nodeIdx].Partition(context.Background(), &pb.PartitionRequest{PeerIds: peerIDs})
	if err != nil {
		return err
	}
	fmt.Printf("node %d (%s) disconnected from nodes %v\n", nodeIdx, c.addrs[nodeIdx], peerIdxs)
	return nil
}

func (c *Cluster) Heal(nodeIdx int) error {
	_, err := c.admin[nodeIdx].Heal(context.Background(), &pb.HealRequest{})
	if err != nil {
		return err
	}
	fmt.Printf("node %d (%s) reconnected\n", nodeIdx, c.addrs[nodeIdx])
	return nil
}

func (c *Cluster) waitForBlocksOnNodes(minSeq uint64, timeout time.Duration, nodeIdxs ...int) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	tick := 0
	for {
		allReady := true
		for _, nodeIdx := range nodeIdxs {
			resp, err := c.ctrl[nodeIdx].GetStatus(ctx, &pb.GetStatusRequest{})
			if err != nil || resp.Seq < minSeq {
				if tick%10 == 0 {
					fmt.Printf("node %d (%s) lagging: seq=%d, want=%d\n", nodeIdx, c.addrs[nodeIdx], resp.GetSeq(), minSeq)
				}
				allReady = false
				break
			}
		}
		if allReady {
			return nil
		}
		tick++
		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for all nodes to reach seq %d", minSeq)
		case <-time.After(200 * time.Millisecond):
		}
	}
}


// Tests 

func testSafety (c *Cluster) error {
	ctx := context.Background()

	for i := 0; i <4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx1")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	} 
	
	if err := c.waitForBlocks(1, 60*time.Second); err != nil {
        return err
    }
	return c.verifyAgreement()
}


func testPartition(c *Cluster) error {
	ctx := context.Background()

	for i :=0; i <4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-before-partition")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	
	if err := c.waitForBlocks(4, 60*time.Second); err != nil {
		return err
	}

	err := c.Partition(3, 0, 1, 2)
	if err != nil {
		return fmt.Errorf("partition node3: %w", err)
	}
	// Wait for the partition to take effect: let any in-flight rounds drain and
	// the gRPC flag settle before driving new progress.
	time.Sleep(3 * time.Second)

	// Record node3's seq — it must not advance while partitioned.
	node3Before, err := c.ctrl[3].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus node3 before partition: %w", err)
	}

	for i := 0; i < 3; i++ {
		_, err = c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-during-partition")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}

	if err := c.waitForBlocksOnNodes(node3Before.Seq+1, 30*time.Second, 0, 1, 2); err != nil {
		return fmt.Errorf("nodes 0-2 failed to progress during partition: %w", err)
	}

	// Assert node3 is genuinely isolated — it must not have advanced.
	node3After, err := c.ctrl[3].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus node3 after partition: %w", err)
	}
	if node3After.Seq != node3Before.Seq {
		return fmt.Errorf("partition failed: node3 advanced from seq=%d to seq=%d while isolated",
			node3Before.Seq, node3After.Seq)
	}

	if err := c.Heal(3); err != nil {
		return fmt.Errorf("heal node3: %w", err)
	}

	for i:=0; i<4; i++ {
		_, err = c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-after-heal")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}

	if err := c.waitForBlocks(node3Before.Seq+2, 60*time.Second); err != nil {
		return err
	}

	if err := c.verifyAgreement(); err != nil {
		return err
	}
	return nil
}



// testPartialPartition partitions node3 from node0 and node1 only, leaving
// its link to node2 alive. With quorum=3-of-4, nodes 0+1+2 can still form
// a quorum, so the cluster must stay live. node3 is still reachable via node2
// and must catch up after healing.
func testPartialPartition(c *Cluster) error {
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-before-partial-partition")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}

	if err := c.waitForBlocks(2, 60*time.Second); err != nil {
		return err
	}

	// Cut node3 off from node0 and node1, but keep node2 reachable.
	if err := c.Partition(3, 0, 1); err != nil {
		return fmt.Errorf("partial partition node3: %w", err)
	}
	time.Sleep(3 * time.Second)

	before, err := c.ctrl[0].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus node0: %w", err)
	}

	for i := 0; i < 4; i++ {
		_, err = c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-during-partial-partition")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}

	// Nodes 0,1,2 must advance — they can still form a quorum of 3.
	if err := c.waitForBlocksOnNodes(before.Seq+1, 30*time.Second, 0, 1, 2); err != nil {
		return fmt.Errorf("cluster failed to progress under partial partition: %w", err)
	}

	if err := c.Heal(3); err != nil {
		return fmt.Errorf("heal node3: %w", err)
	}

	for i := 0; i < 4; i++ {
		_, err = c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-after-partial-heal")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}

	// All 4 nodes must converge after healing.
	if err := c.waitForBlocks(before.Seq+2, 60*time.Second); err != nil {
		return err
	}

	return c.verifyAgreement()
}

// testNetSplit creates a symmetric 2-2 partition: side A={0,1}, side B={2,3}.
// With quorum=3-of-4, neither side can make progress. After healing all 4 nodes
// must agree on all committed blocks.
func testNetSplit(c *Cluster) error {
	ctx := context.Background()

	// Baseline: submit tx to all 4 and wait for ≥2 committed blocks.
	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-before-netsplit")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}

	if err := c.waitForBlocks(2, 60*time.Second); err != nil {
		return err
	}

	// Symmetric 2-2 split: each node drops messages from the other side.
	if err := c.Partition(0, 2, 3); err != nil {
		return fmt.Errorf("partition node0 from {2,3}: %w", err)
	}
	if err := c.Partition(1, 2, 3); err != nil {
		return fmt.Errorf("partition node1 from {2,3}: %w", err)
	}
	if err := c.Partition(2, 0, 1); err != nil {
		return fmt.Errorf("partition node2 from {0,1}: %w", err)
	}
	if err := c.Partition(3, 0, 1); err != nil {
		return fmt.Errorf("partition node3 from {0,1}: %w", err)
	}

	// Let any in-flight messages drain before snapshotting.
	time.Sleep(1 * time.Second)

	// Snapshot seq on all nodes.
	seqBefore := make([]uint64, 4)
	for i := 0; i < 4; i++ {
		resp, err := c.ctrl[i].GetStatus(ctx, &pb.GetStatusRequest{})
		if err != nil {
			return fmt.Errorf("GetStatus node%d before stall: %w", i, err)
		}
		seqBefore[i] = resp.Seq
	}

	// Stall window: neither side can reach quorum=3.
	time.Sleep(5 * time.Second)

	// Assert no node advanced — safety violation if any did.
	for i := 0; i < 4; i++ {
		resp, err := c.ctrl[i].GetStatus(ctx, &pb.GetStatusRequest{})
		if err != nil {
			return fmt.Errorf("GetStatus node%d after stall: %w", i, err)
		}
		if resp.Seq != seqBefore[i] {
			return fmt.Errorf("SAFETY VIOLATION: node%d advanced from seq=%d to seq=%d during 2-2 netsplit",
				i, seqBefore[i], resp.Seq)
		}
	}
	fmt.Println("netsplit stall verified: no node advanced during 2-2 split")

	// Heal all partitions (empty HealRequest = heal all peers).
	for i := 0; i < 4; i++ {
		if err := c.Heal(i); err != nil {
			return fmt.Errorf("heal node%d: %w", i, err)
		}
	}

	// Submit fresh tx to all 4 nodes to unblock BuildBlock.
	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-after-netsplit")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}

	// All nodes must advance past their pre-split seq.
	maxBefore := seqBefore[0]
	for _, s := range seqBefore[1:] {
		if s > maxBefore {
			maxBefore = s
		}
	}
	if err := c.waitForBlocks(maxBefore+2, 60*time.Second); err != nil {
		return fmt.Errorf("cluster failed to recover after netsplit: %w", err)
	}

	// All 4 nodes must agree on every committed block.
	return c.verifyAgreement()
}

func testRePartition (c *Cluster) error {
	ctx := context.Background()

		// Baseline: submit tx to all 4 and wait for ≥2 committed blocks.
	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-before-netsplit")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}

	if err := c.waitForBlocks(2, 60*time.Second); err != nil {
		return err
	}

	if err := c.Partition(3, 0, 1, 2); err != nil {
		return fmt.Errorf("partition node3 from {0,1}: %w", err)
	}

	before, err := c.ctrl[3].GetStatus(ctx, &pb.GetStatusRequest{})

	if err != nil {
		return fmt.Errorf("GetStatus node3 after stall: %w", err)
	}

	for i := 0; i < 3; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-before-netsplit")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}

	if err := c.waitForBlocksOnNodes(8, 60*time.Second, 0, 1, 2); err != nil {
		return err
	}

	resp, err := c.ctrl[3].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus node3 after partition: %w", err)
	}
	if resp.Seq != before.Seq {
		return fmt.Errorf("SAFETY VIOLATION: node3 advanced from seq=%d to seq=%d while partitioned",
			before.Seq, resp.Seq)
	}
	if err := c.Heal(3); err != nil {
		return fmt.Errorf("heal node3: %w", err)
	}

	time.Sleep(300 * time.Millisecond)
	if err := c.Partition(3, 0, 1, 2); err != nil {
		return fmt.Errorf("re-partition node3: %w", err)
	}
	time.Sleep(2 * time.Second)

	if err := c.Heal(3); err != nil {
		return fmt.Errorf("heal node3: %w", err)
	}



	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-before-netsplit")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}


	if err := c.waitForBlocks(9, 60*time.Second); err != nil {
		return fmt.Errorf("cluster failed to recover after netsplit: %w", err)
	}

	return c.verifyAgreement()

}

// testCascadingPartition progressively removes nodes until the cluster falls
// below quorum, verifies it stalls, then restores quorum one node at a time
// and verifies recovery and agreement at each step.
//
//   4-of-4  →  partition node0  →  3-of-4 (live)
//           →  partition node1  →  2-of-4 (stall)
//           →  heal node0       →  3-of-4 (live again)
//           →  heal node1       →  4-of-4 (full recovery)
func testCascadingPartition(c *Cluster) error {
	ctx := context.Background()

	// Baseline: submit tx to all 4, wait for ≥2 blocks.
	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-before-cascade")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(2, 60*time.Second); err != nil {
		return err
	}

	// --- Phase 1: isolate node0 — 3-of-4 remain, cluster must stay live ---
	if err := c.Partition(0, 1, 2, 3); err != nil {
		return fmt.Errorf("partition node0: %w", err)
	}
	time.Sleep(1 * time.Second)

	node0Before, err := c.ctrl[0].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus node0: %w", err)
	}

	for i := 1; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-phase1")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocksOnNodes(node0Before.Seq+2, 30*time.Second, 1, 2, 3); err != nil {
		return fmt.Errorf("nodes 1-3 failed to progress with node0 isolated: %w", err)
	}
	node0Check, err := c.ctrl[0].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus node0 check: %w", err)
	}
	if node0Check.Seq != node0Before.Seq {
		return fmt.Errorf("SAFETY VIOLATION: node0 advanced from seq=%d to seq=%d while isolated",
			node0Before.Seq, node0Check.Seq)
	}
	fmt.Println("phase1: node0 isolated, nodes 1-3 advancing — OK")

	// --- Phase 2: additionally isolate node1 — 2-of-4 remain, below quorum → stall ---
	node1BeforePhase2, err := c.ctrl[1].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus node1 before phase2: %w", err)
	}
	if err := c.Partition(1, 0, 2, 3); err != nil {
		return fmt.Errorf("partition node1: %w", err)
	}
	time.Sleep(1 * time.Second)

	seq2, err := c.ctrl[2].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus node2 before stall: %w", err)
	}
	seq3, err := c.ctrl[3].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus node3 before stall: %w", err)
	}
	seqStall := seq2.Seq
	if seq3.Seq > seqStall {
		seqStall = seq3.Seq
	}

	time.Sleep(5 * time.Second)

	for i, snapSeq := range []uint64{seq2.Seq, seq3.Seq} {
		nodeIdx := i + 2
		resp, err := c.ctrl[nodeIdx].GetStatus(ctx, &pb.GetStatusRequest{})
		if err != nil {
			return fmt.Errorf("GetStatus node%d after stall: %w", nodeIdx, err)
		}
		if resp.Seq != snapSeq {
			return fmt.Errorf("SAFETY VIOLATION: node%d advanced from seq=%d to seq=%d with only 2-of-4 nodes connected",
				nodeIdx, snapSeq, resp.Seq)
		}
	}
	fmt.Println("phase2: 2-of-4 stall verified — no node advanced below quorum")

	// --- Phase 3: heal node0 — {0,2,3} = 3-of-4, quorum restored ---
	if err := c.Heal(0); err != nil {
		return fmt.Errorf("heal node0: %w", err)
	}
	for _, i := range []int{0, 2, 3} {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-phase3")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocksOnNodes(seqStall+2, 60*time.Second, 0, 2, 3); err != nil {
		return fmt.Errorf("nodes {0,2,3} failed to recover after healing node0: %w", err)
	}
	node1AfterPhase3, err := c.ctrl[1].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus node1 after phase3: %w", err)
	}
	if node1AfterPhase3.Seq != node1BeforePhase2.Seq {
		return fmt.Errorf("SAFETY VIOLATION: node1 advanced from seq=%d to seq=%d while still isolated during phase3",
			node1BeforePhase2.Seq, node1AfterPhase3.Seq)
	}
	fmt.Printf("phase3: {0,2,3} recovered to seq>=%d, node1 still at seq=%d — OK\n", seqStall+2, node1AfterPhase3.Seq)

	// --- Phase 4: heal node1 — full cluster recovers ---
	if err := c.Heal(1); err != nil {
		return fmt.Errorf("heal node1: %w", err)
	}
	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-after-cascade")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(seqStall+3, 60*time.Second); err != nil {
		return fmt.Errorf("cluster failed to fully recover after cascading partition: %w", err)
	}

	return c.verifyAgreement()
}

// testRollingPartition isolates each node in turn (0→1→2→3) while the remaining
// 3 advance, then heals and verifies the isolated node catches up before moving
// on to the next. All 4 nodes must agree at the end.
func testRollingPartition(c *Cluster) error {
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-before-partition")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(4, 60*time.Second); err != nil {
		return err
	}

	for nodeIdx := 0; nodeIdx < 4; nodeIdx++ {
		peers := make([]int, 0, 3)
		for i := 0; i < 4; i++ {
			if i != nodeIdx {
				peers = append(peers, i)
			}
		}

		if err := c.Partition(nodeIdx, peers...); err != nil {
			return fmt.Errorf("partition node%d: %w", nodeIdx, err)
		}
		time.Sleep(3 * time.Second)

		nodeBefore, err := c.ctrl[nodeIdx].GetStatus(ctx, &pb.GetStatusRequest{})
		if err != nil {
			return fmt.Errorf("GetStatus node%d before partition: %w", nodeIdx, err)
		}

		for _, p := range peers {
			_, err = c.ctrl[p].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-during-partition")})
			if err != nil {
				return fmt.Errorf("SubmitTransaction to node%d: %w", p, err)
			}
		}

		if err := c.waitForBlocksOnNodes(nodeBefore.Seq+1, 30*time.Second, peers...); err != nil {
			return fmt.Errorf("peers failed to progress with node%d isolated: %w", nodeIdx, err)
		}

		nodeAfter, err := c.ctrl[nodeIdx].GetStatus(ctx, &pb.GetStatusRequest{})
		if err != nil {
			return fmt.Errorf("GetStatus node%d after partition: %w", nodeIdx, err)
		}
		if nodeAfter.Seq != nodeBefore.Seq {
			return fmt.Errorf("SAFETY VIOLATION: node%d advanced from seq=%d to seq=%d while isolated",
				nodeIdx, nodeBefore.Seq, nodeAfter.Seq)
		}

		if err := c.Heal(nodeIdx); err != nil {
			return fmt.Errorf("heal node%d: %w", nodeIdx, err)
		}

		for i := 0; i < 4; i++ {
			_, err = c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-after-heal")})
			if err != nil {
				return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
			}
		}

		if err := c.waitForBlocks(nodeBefore.Seq+2, 60*time.Second); err != nil {
			return fmt.Errorf("cluster failed to recover after partitioning node%d: %w", nodeIdx, err)
		}

		fmt.Printf("node%d partition cycle complete\n", nodeIdx)
	}

	return c.verifyAgreement()
}

// testCrossMinorityPartition blocks direct communication between node2 and node3
// while keeping both connected to node0 and node1. With quorum=3-of-4, any
// three nodes that include at least one of {node0,node1} can still assemble a
// notarization, so the cluster must remain fully live. Agreement must hold
// throughout — the key difference from isolation tests: here we assert all 4
// nodes advance, not that some node is frozen.
func testCrossMinorityPartition(c *Cluster) error {
	ctx := context.Background()

	// Baseline: submit tx to all 4, wait for ≥2 blocks.
	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-before-cross-partition")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(2, 60*time.Second); err != nil {
		return err
	}

	// Cut node2↔node3 in both directions, leave node0 and node1 fully connected.
	if err := c.Partition(2, 3); err != nil {
		return fmt.Errorf("partition node2 from node3: %w", err)
	}
	if err := c.Partition(3, 2); err != nil {
		return fmt.Errorf("partition node3 from node2: %w", err)
	}
	time.Sleep(1 * time.Second)

	before, err := c.ctrl[0].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus node0: %w", err)
	}

	// Submit tx to all 4 nodes, including the two that can't see each other.
	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-during-cross-partition")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}

	// All 4 nodes must advance — quorum is still achievable via node0/node1.
	if err := c.waitForBlocks(before.Seq+3, 60*time.Second); err != nil {
		return fmt.Errorf("cluster failed to progress during cross-minority partition: %w", err)
	}
	fmt.Println("cross-minority partition: all 4 nodes advanced — liveness holds")

	// Verify no fork occurred despite the blind spot.
	if err := c.verifyAgreement(); err != nil {
		return err
	}

	// Heal the blind spot and confirm full recovery.
	if err := c.Heal(2); err != nil {
		return fmt.Errorf("heal node2: %w", err)
	}
	if err := c.Heal(3); err != nil {
		return fmt.Errorf("heal node3: %w", err)
	}

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-after-cross-partition")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(before.Seq+5, 60*time.Second); err != nil {
		return fmt.Errorf("cluster failed to recover after cross-minority partition: %w", err)
	}

	return c.verifyAgreement()
}

// testLeaderAssassination repeatedly partitions the node that is expected to be
// the next round's leader, forcing the remaining 3 to time out (MaxProposalWait=2s)
// and advance via empty votes. After each assassination the leader is healed and
// all 4 nodes reconverge before the next cycle. 8 attempts cover each of the 4
// leader slots at least twice.
func testLeaderAssassination(c *Cluster) error {
	ctx := context.Background()

	// Build the sorted node index order used by LeaderForRound in simplex/epoch.go.
	// Nodes are sorted by byte value; lowercase hex strings of equal length sort
	// identically to their underlying bytes, so string comparison is correct here.
	sortedIndices := make([]int, len(c.nodeIDs))
	for i := range sortedIndices {
		sortedIndices[i] = i
	}
	sort.Slice(sortedIndices, func(i, j int) bool {
		return c.nodeIDs[sortedIndices[i]] < c.nodeIDs[sortedIndices[j]]
	})
	leaderFor := func(round uint64) int {
		return sortedIndices[round%uint64(len(sortedIndices))]
	}

	// Baseline: wait for ≥2 committed blocks.
	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-before-assassination")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(2, 60*time.Second); err != nil {
		return err
	}

	const attempts = 8
	for attempt := 0; attempt < attempts; attempt++ {
		// Sample the current round from node0.
		status, err := c.ctrl[0].GetStatus(ctx, &pb.GetStatusRequest{})
		if err != nil {
			return fmt.Errorf("GetStatus attempt %d: %w", attempt, err)
		}

		// Partition the expected leader of the next round before it can propose.
		targetRound := status.Round + 1
		leaderIdx := leaderFor(targetRound)

		peers := make([]int, 0, 3)
		for i := 0; i < 4; i++ {
			if i != leaderIdx {
				peers = append(peers, i)
			}
		}

		fmt.Printf("assassination %d: partitioning node%d (leader for round %d)\n", attempt, leaderIdx, targetRound)

		if err := c.Partition(leaderIdx, peers...); err != nil {
			return fmt.Errorf("partition leader node%d: %w", leaderIdx, err)
		}

		// Submit tx to the non-leader peers so they have work once they elect a new leader.
		for _, p := range peers {
			_, err := c.ctrl[p].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{
				Data: []byte(fmt.Sprintf("tx-assassination-%d", attempt)),
			})
			if err != nil {
				return fmt.Errorf("SubmitTransaction to node%d: %w", p, err)
			}
		}

		// Non-leaders must advance: they will time out after MaxProposalWait (2s)
		// and proceed via empty votes, then commit a block in the following round.
		if err := c.waitForBlocksOnNodes(status.Seq+1, 60*time.Second, peers...); err != nil {
			return fmt.Errorf("peers failed to advance after assassination %d (leader=node%d): %w", attempt, leaderIdx, err)
		}

		if err := c.Heal(leaderIdx); err != nil {
			return fmt.Errorf("heal node%d: %w", leaderIdx, err)
		}

		// Reconverge all 4 before the next assassination.
		for i := 0; i < 4; i++ {
			_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-post-assassination")})
			if err != nil {
				return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
			}
		}
		if err := c.waitForBlocks(status.Seq+2, 60*time.Second); err != nil {
			return fmt.Errorf("cluster failed to recover after assassination %d: %w", attempt, err)
		}

		fmt.Printf("assassination %d: cluster recovered — OK\n", attempt)
	}

	return c.verifyAgreement()
}

// testFlappingPartition rapidly toggles node3's connection (300ms down / 300ms up,
// 10 cycles) while nodes 0-2 keep committing blocks. Each reconnection forces the
// replication machinery to resync partial state. After the flap storm node3 must
// catch up fully and all 4 nodes must agree.
func testFlappingPartition(c *Cluster) error {
	ctx := context.Background()

	// Baseline: wait for ≥2 blocks so there is history to catch up on.
	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-before-flap")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(2, 60*time.Second); err != nil {
		return err
	}

	before, err := c.ctrl[0].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus before flap: %w", err)
	}

	const (
		flapCycles = 10
		flapDown   = 300 * time.Millisecond // well below MaxProposalWait (2s)
		flapUp     = 300 * time.Millisecond
	)

	for i := 0; i < flapCycles; i++ {
		if err := c.Partition(3, 0, 1, 2); err != nil {
			return fmt.Errorf("flap %d partition: %w", i, err)
		}
		time.Sleep(flapDown)
		if err := c.Heal(3); err != nil {
			return fmt.Errorf("flap %d heal: %w", i, err)
		}
		time.Sleep(flapUp)
		// Keep the stable nodes fed with work; ignore errors during the up window
		// since a tx submit can race with the next down phase.
		for _, p := range []int{0, 1, 2} {
			c.ctrl[p].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{ //nolint
				Data: []byte(fmt.Sprintf("tx-flap-%d", i)),
			})
		}
	}
	fmt.Printf("flapping complete: %d cycles of %v down / %v up on node3\n", flapCycles, flapDown, flapUp)

	// Nodes 0-2 must have advanced during the flap storm.
	if err := c.waitForBlocksOnNodes(before.Seq+3, 30*time.Second, 0, 1, 2); err != nil {
		return fmt.Errorf("nodes 0-2 failed to progress during flapping: %w", err)
	}

	// Submit to all 4 and wait for node3 to fully catch up.
	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-after-flap")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(before.Seq+5, 60*time.Second); err != nil {
		return fmt.Errorf("cluster failed to converge after flapping: %w", err)
	}

	return c.verifyAgreement()
}

