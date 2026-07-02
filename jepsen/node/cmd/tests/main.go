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
	{"single-crash", testSingleCrash},
	{"rolling-crash", testRollingCrash},
	{"repeated-crash", testRepeatedCrash},
	{"crash-during-replication", testCrashDuringReplication},
	{"leader-crash", testLeaderCrash},
	{"simultaneous-crash", testSimultaneousCrash},
	{"full-cluster-restart", testFullClusterRestart},
	{"drop-votes", testDropVotes},
	{"drop-replication-response", testDropReplicationResponse},
	{"drop-block-message", testDropBlockMessage},
	{"drop-finalize-vote", testDropFinalizeVote},
	{"drop-empty-vote", testDropEmptyVote},
}

type Cluster struct {
	dir       string
	configDir string
	nodeBin   string
	procs     []*os.Process
	addrs     []string
	nodeIDs   []string
	ctrl      []pb.ControlServiceClient
	admin     []pb.AdminServiceClient
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
		dir:       dir,
		configDir: configDir,
		nodeBin:   bins.nodeBin,
		procs:     procs,
		addrs:     addrs,
		nodeIDs:   nodeIDs,
		ctrl:      ctrlClients,
		admin:     adminClients,
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

func (c *Cluster) TearDown() {
	for _, p := range c.procs {
		p.Kill()
	}
	os.RemoveAll(c.dir)
}

// Kill sends SIGKILL to node i, simulating a hard crash with no cleanup.
func (c *Cluster) Kill(i int) error {
	if err := c.procs[i].Kill(); err != nil {
		return fmt.Errorf("kill node%d: %w", i, err)
	}
	c.procs[i].Wait() // reap the zombie so the port is released
	fmt.Printf("node %d (%s) killed\n", i, c.addrs[i])
	return nil
}

// Restart relaunches node i from its on-disk config (same address, same WAL).
// The node replays committed state from storage and rejoins the cluster.
func (c *Cluster) Restart(i int) error {
	cfgPath := filepath.Join(c.configDir, fmt.Sprintf("node%d", i), "config.json")
	cmd := exec.Command(c.nodeBin, "-config", cfgPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("restart node%d: %w", i, err)
	}
	c.procs[i] = cmd.Process

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := waitForNodes(ctx, []string{c.addrs[i]}); err != nil {
		cmd.Process.Kill()
		return fmt.Errorf("node%d did not become ready after restart: %w", i, err)
	}
	fmt.Printf("node %d (%s) restarted\n", i, c.addrs[i])
	return nil
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

// DropMessages installs a message-type filter on node nodeIdx: any incoming
// message whose type is in msgTypes is silently dropped before reaching the engine.
// Valid type names: block_message, vote_message, empty_vote_message, notarization,
// empty_notarization, finalize_vote, finalization, replication_request,
// replication_response, block_digest_request.
func (c *Cluster) DropMessages(nodeIdx int, msgTypes ...string) error {
	_, err := c.admin[nodeIdx].SetMessageFilter(context.Background(), &pb.SetMessageFilterRequest{
		DropTypes: msgTypes,
	})
	if err != nil {
		return fmt.Errorf("SetMessageFilter node%d: %w", nodeIdx, err)
	}
	fmt.Printf("node %d (%s) dropping message types: %v\n", nodeIdx, c.addrs[nodeIdx], msgTypes)
	return nil
}

// ClearMessages removes all message-type filters on node nodeIdx.
func (c *Cluster) ClearMessages(nodeIdx int) error {
	_, err := c.admin[nodeIdx].SetMessageFilter(context.Background(), &pb.SetMessageFilterRequest{})
	if err != nil {
		return fmt.Errorf("ClearMessageFilter node%d: %w", nodeIdx, err)
	}
	fmt.Printf("node %d (%s) message filter cleared\n", nodeIdx, c.addrs[nodeIdx])
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


// Partition Tests 

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


// testSingleCrash kills node3 with SIGKILL, lets the remaining 3 advance,
// then restarts it. On restart the node recovers its committed seq from bbolt
// (the WAL check), replicates only the missing blocks, and must agree with
// the rest of the cluster.
func testSingleCrash(c *Cluster) error {
	ctx := context.Background()

	// Baseline: commit ≥4 blocks so there is real history in the WAL.
	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-before-crash")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(4, 60*time.Second); err != nil {
		return err
	}

	// Snapshot node3's seq before the crash — used for the WAL check later.
	node3PreCrash, err := c.ctrl[3].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus node3 pre-crash: %w", err)
	}

	if err := c.Kill(3); err != nil {
		return err
	}

	// Nodes 0-2 advance 3 more blocks while node3 is dead.
	for i := 0; i < 3; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-during-crash")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocksOnNodes(node3PreCrash.Seq+3, 30*time.Second, 0, 1, 2); err != nil {
		return fmt.Errorf("nodes 0-2 failed to progress while node3 was crashed: %w", err)
	}

	// Restart node3. bbolt recovers numBlocks and lastIndexedDigest on open.
	if err := c.Restart(3); err != nil {
		return err
	}

	// WAL check: node3 must resume from its pre-crash seq, not from 0.
	node3PostRestart, err := c.ctrl[3].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus node3 post-restart: %w", err)
	}
	if node3PostRestart.Seq < node3PreCrash.Seq {
		return fmt.Errorf("WAL recovery failed: node3 restarted at seq=%d, expected >=%d",
			node3PostRestart.Seq, node3PreCrash.Seq)
	}
	fmt.Printf("WAL recovery OK: node3 resumed at seq=%d (pre-crash seq=%d)\n",
		node3PostRestart.Seq, node3PreCrash.Seq)

	// Drive convergence: node3 must replicate the gap and rejoin consensus.
	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-after-restart")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(node3PreCrash.Seq+4, 60*time.Second); err != nil {
		return fmt.Errorf("cluster failed to converge after restart: %w", err)
	}

	return c.verifyAgreement()
}

// testRollingCrash kills each node in turn (0→1→2→3) while the remaining 3
// keep committing blocks. Each crashed node is restarted, its WAL recovery
// is verified, and the cluster must fully reconverge before the next crash.
func testRollingCrash(c *Cluster) error {
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-before-rolling-crash")})
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

		preCrash, err := c.ctrl[nodeIdx].GetStatus(ctx, &pb.GetStatusRequest{})
		if err != nil {
			return fmt.Errorf("GetStatus node%d pre-crash: %w", nodeIdx, err)
		}

		if err := c.Kill(nodeIdx); err != nil {
			return err
		}

		for _, p := range peers {
			_, err := c.ctrl[p].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{
				Data: []byte(fmt.Sprintf("tx-rolling-crash-%d", nodeIdx)),
			})
			if err != nil {
				return fmt.Errorf("SubmitTransaction to node%d: %w", p, err)
			}
		}
		if err := c.waitForBlocksOnNodes(preCrash.Seq+3, 30*time.Second, peers...); err != nil {
			return fmt.Errorf("peers failed to progress while node%d was crashed: %w", nodeIdx, err)
		}

		if err := c.Restart(nodeIdx); err != nil {
			return err
		}

		postRestart, err := c.ctrl[nodeIdx].GetStatus(ctx, &pb.GetStatusRequest{})
		if err != nil {
			return fmt.Errorf("GetStatus node%d post-restart: %w", nodeIdx, err)
		}
		if postRestart.Seq < preCrash.Seq {
			return fmt.Errorf("WAL recovery failed: node%d restarted at seq=%d, expected >=%d",
				nodeIdx, postRestart.Seq, preCrash.Seq)
		}
		fmt.Printf("node%d WAL recovery OK: resumed at seq=%d (pre-crash seq=%d)\n",
			nodeIdx, postRestart.Seq, preCrash.Seq)

		for i := 0; i < 4; i++ {
			_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-after-rolling-crash")})
			if err != nil {
				return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
			}
		}
		if err := c.waitForBlocks(preCrash.Seq+4, 60*time.Second); err != nil {
			return fmt.Errorf("cluster failed to converge after crashing node%d: %w", nodeIdx, err)
		}

		fmt.Printf("node%d rolling crash cycle complete\n", nodeIdx)
	}

	return c.verifyAgreement()
}

// testRepeatedCrash crashes the same node (node3) 5 times in succession,
// accumulating new committed blocks between each crash. Tests that repeated
// WAL recovery from an increasingly deep history doesn't corrupt state.
func testRepeatedCrash(c *Cluster) error {
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-before-repeated-crash")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(4, 60*time.Second); err != nil {
		return err
	}

	const crashCount = 5
	for cycle := 0; cycle < crashCount; cycle++ {
		preCrash, err := c.ctrl[3].GetStatus(ctx, &pb.GetStatusRequest{})
		if err != nil {
			return fmt.Errorf("GetStatus node3 cycle %d pre-crash: %w", cycle, err)
		}

		if err := c.Kill(3); err != nil {
			return err
		}

		for i := 0; i < 3; i++ {
			_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{
				Data: []byte(fmt.Sprintf("tx-repeated-crash-%d", cycle)),
			})
			if err != nil {
				return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
			}
		}
		if err := c.waitForBlocksOnNodes(preCrash.Seq+2, 30*time.Second, 0, 1, 2); err != nil {
			return fmt.Errorf("nodes 0-2 failed to progress in cycle %d: %w", cycle, err)
		}

		if err := c.Restart(3); err != nil {
			return err
		}

		postRestart, err := c.ctrl[3].GetStatus(ctx, &pb.GetStatusRequest{})
		if err != nil {
			return fmt.Errorf("GetStatus node3 cycle %d post-restart: %w", cycle, err)
		}
		if postRestart.Seq < preCrash.Seq {
			return fmt.Errorf("cycle %d WAL recovery failed: node3 at seq=%d, expected >=%d",
				cycle, postRestart.Seq, preCrash.Seq)
		}

		for i := 0; i < 4; i++ {
			_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-after-repeated-crash")})
			if err != nil {
				return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
			}
		}
		if err := c.waitForBlocks(preCrash.Seq+3, 60*time.Second); err != nil {
			return fmt.Errorf("cluster failed to converge in cycle %d: %w", cycle, err)
		}

		fmt.Printf("repeated crash cycle %d/%d complete: node3 seq>=%d\n", cycle+1, crashCount, preCrash.Seq+3)
	}

	return c.verifyAgreement()
}

// testCrashDuringReplication crashes node3 while it is actively catching up
// after a large gap. node3 is first partitioned so it falls 8 blocks behind,
// then killed. On restart it begins replicating; after ~1s (mid-replication)
// it is killed again. The second restart must fully recover and rejoin consensus.
func testCrashDuringReplication(c *Cluster) error {
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-before-crash-replication")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(4, 60*time.Second); err != nil {
		return err
	}

	// Partition node3 so it doesn't see new blocks.
	if err := c.Partition(3, 0, 1, 2); err != nil {
		return fmt.Errorf("partition node3: %w", err)
	}

	preCrash, err := c.ctrl[3].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus node3 before gap: %w", err)
	}

	// Drive 8 blocks on nodes 0-2 to create a large replication gap.
	for round := 0; round < 8; round++ {
		for i := 0; i < 3; i++ {
			_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{
				Data: []byte(fmt.Sprintf("tx-gap-%d", round)),
			})
			if err != nil {
				return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
			}
		}
	}
	if err := c.waitForBlocksOnNodes(preCrash.Seq+8, 60*time.Second, 0, 1, 2); err != nil {
		return fmt.Errorf("nodes 0-2 failed to build gap: %w", err)
	}
	fmt.Printf("gap created: nodes 0-2 at seq>=%d, node3 still at seq=%d\n", preCrash.Seq+8, preCrash.Seq)

	// Kill node3 while still partitioned (process kill clears network state on restart).
	if err := c.Kill(3); err != nil {
		return err
	}

	// First restart: node3 begins replicating the 8-block gap.
	if err := c.Restart(3); err != nil {
		return err
	}

	// Kill mid-replication.
	time.Sleep(1 * time.Second)
	if err := c.Kill(3); err != nil {
		return err
	}
	fmt.Println("node3 killed mid-replication")

	// Second restart: must recover from partially-applied replication state.
	if err := c.Restart(3); err != nil {
		return err
	}

	postRestart, err := c.ctrl[3].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus node3 after second restart: %w", err)
	}
	if postRestart.Seq < preCrash.Seq {
		return fmt.Errorf("second restart regression: node3 at seq=%d, expected >=%d",
			postRestart.Seq, preCrash.Seq)
	}
	fmt.Printf("second restart OK: node3 at seq=%d (gap start was seq=%d)\n", postRestart.Seq, preCrash.Seq)

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-after-crash-replication")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(preCrash.Seq+10, 60*time.Second); err != nil {
		return fmt.Errorf("cluster failed to converge after crash-during-replication: %w", err)
	}

	return c.verifyAgreement()
}

// testLeaderCrash SIGKILLs the expected next-round leader before it can complete
// its proposal broadcast. Unlike leader-assassination (which partitions the leader),
// a killed leader may have sent BlockMessage to some peers but not others before dying,
// leaving the round in a partially-broadcast state. The surviving 3 nodes must
// advance via empty votes (MaxProposalWait=2s) and the restarted leader must recover.
func testLeaderCrash(c *Cluster) error {
	ctx := context.Background()

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

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-before-leader-crash")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(4, 60*time.Second); err != nil {
		return err
	}

	status, err := c.ctrl[0].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus: %w", err)
	}

	targetRound := status.Round + 1
	leaderIdx := leaderFor(targetRound)
	peers := make([]int, 0, 3)
	for i := 0; i < 4; i++ {
		if i != leaderIdx {
			peers = append(peers, i)
		}
	}

	preCrash, err := c.ctrl[leaderIdx].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus leader node%d: %w", leaderIdx, err)
	}

	fmt.Printf("killing leader node%d (expected leader for round %d)\n", leaderIdx, targetRound)
	if err := c.Kill(leaderIdx); err != nil {
		return err
	}

	for _, p := range peers {
		_, err := c.ctrl[p].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-after-leader-crash")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", p, err)
		}
	}

	// Surviving nodes advance via empty votes after MaxProposalWait (2s).
	if err := c.waitForBlocksOnNodes(status.Seq+1, 60*time.Second, peers...); err != nil {
		return fmt.Errorf("surviving peers failed to advance after leader crash: %w", err)
	}

	if err := c.Restart(leaderIdx); err != nil {
		return err
	}

	postRestart, err := c.ctrl[leaderIdx].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus leader post-restart: %w", err)
	}
	if postRestart.Seq < preCrash.Seq {
		return fmt.Errorf("WAL recovery failed: leader node%d at seq=%d, expected >=%d",
			leaderIdx, postRestart.Seq, preCrash.Seq)
	}
	fmt.Printf("leader WAL recovery OK: node%d at seq=%d (pre-crash seq=%d)\n",
		leaderIdx, postRestart.Seq, preCrash.Seq)

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-after-leader-restart")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(status.Seq+2, 60*time.Second); err != nil {
		return fmt.Errorf("cluster failed to converge after leader crash: %w", err)
	}

	return c.verifyAgreement()
}

// testSimultaneousCrash kills node2 and node3 at the same time, dropping to
// 2-of-4 live nodes (below quorum=3). Verifies the cluster stalls, then restarts
// both and confirms full recovery. Tests quorum math under a double failure.
func testSimultaneousCrash(c *Cluster) error {
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-before-simultaneous-crash")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(4, 60*time.Second); err != nil {
		return err
	}

	if err := c.Kill(2); err != nil {
		return err
	}
	if err := c.Kill(3); err != nil {
		return err
	}

	seq0, err := c.ctrl[0].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus node0: %w", err)
	}
	seq1, err := c.ctrl[1].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus node1: %w", err)
	}

	time.Sleep(5 * time.Second)

	after0, err := c.ctrl[0].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus node0 after stall: %w", err)
	}
	after1, err := c.ctrl[1].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus node1 after stall: %w", err)
	}
	if after0.Seq != seq0.Seq {
		return fmt.Errorf("SAFETY VIOLATION: node0 advanced from seq=%d to seq=%d with only 2-of-4 nodes alive",
			seq0.Seq, after0.Seq)
	}
	if after1.Seq != seq1.Seq {
		return fmt.Errorf("SAFETY VIOLATION: node1 advanced from seq=%d to seq=%d with only 2-of-4 nodes alive",
			seq1.Seq, after1.Seq)
	}
	fmt.Println("simultaneous crash stall verified: 2-of-4 cannot make progress")

	if err := c.Restart(2); err != nil {
		return err
	}
	if err := c.Restart(3); err != nil {
		return err
	}

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-after-simultaneous-crash")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(seq0.Seq+2, 60*time.Second); err != nil {
		return fmt.Errorf("cluster failed to converge after simultaneous crash: %w", err)
	}

	return c.verifyAgreement()
}

// testFullClusterRestart kills all 4 nodes and restarts them sequentially.
// Each node must recover its committed state from bbolt alone. Because nodes
// restart sequentially, quorum (3-of-4) forms as the third node comes up,
// so nodes restarted before quorum must wait in their WAL-recovered state
// until enough peers are live to resume consensus.
func testFullClusterRestart(c *Cluster) error {
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-before-full-restart")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(8, 60*time.Second); err != nil {
		return err
	}

	preSeqs := make([]uint64, 4)
	for i := 0; i < 4; i++ {
		resp, err := c.ctrl[i].GetStatus(ctx, &pb.GetStatusRequest{})
		if err != nil {
			return fmt.Errorf("GetStatus node%d pre-shutdown: %w", i, err)
		}
		preSeqs[i] = resp.Seq
	}
	fmt.Printf("cluster state before shutdown: seqs=%v\n", preSeqs)

	for i := 0; i < 4; i++ {
		if err := c.Kill(i); err != nil {
			return err
		}
	}
	fmt.Println("all 4 nodes killed")

	for i := 0; i < 4; i++ {
		if err := c.Restart(i); err != nil {
			return err
		}
	}

	for i := 0; i < 4; i++ {
		resp, err := c.ctrl[i].GetStatus(ctx, &pb.GetStatusRequest{})
		if err != nil {
			return fmt.Errorf("GetStatus node%d post-restart: %w", i, err)
		}
		if resp.Seq < preSeqs[i] {
			return fmt.Errorf("WAL regression: node%d at seq=%d, expected >=%d",
				i, resp.Seq, preSeqs[i])
		}
		fmt.Printf("node%d cold-start OK: seq=%d (pre-crash seq=%d)\n", i, resp.Seq, preSeqs[i])
	}

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-after-full-restart")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}

	maxPreSeq := preSeqs[0]
	for _, s := range preSeqs[1:] {
		if s > maxPreSeq {
			maxPreSeq = s
		}
	}
	if err := c.waitForBlocks(maxPreSeq+2, 60*time.Second); err != nil {
		return fmt.Errorf("cluster failed to converge after full restart: %w", err)
	}

	return c.verifyAgreement()
}

// testDropVotes drops all incoming vote_message on every node. No notarization
// QC can form, so each round times out and advances via empty votes (no blocks
// committed). The key assertions are:
//   - rounds advance (empty-vote path is live)
//   - seq stays frozen (empty rounds do not commit blocks)
//
// After clearing the filter, real voting resumes and blocks are committed.
func testDropVotes(c *Cluster) error {
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-before-drop-votes")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(4, 60*time.Second); err != nil {
		return err
	}

	// Drop vote_message on all 4 nodes — no notarization QC can form.
	for i := 0; i < 4; i++ {
		if err := c.DropMessages(i, "vote_message"); err != nil {
			return err
		}
	}

	// Wait one MaxProposalWait for any in-flight votes to resolve before
	// snapshotting — otherwise a round already past quorum could commit seq+1.
	time.Sleep(3 * time.Second)

	before, err := c.ctrl[0].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus after filter applied: %w", err)
	}

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-during-drop-votes")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}

	// Wait for ~5 more empty-vote rounds (each takes MaxProposalWait ≈ 2s).
	time.Sleep(11 * time.Second)

	// Verify: rounds must have advanced (empty-vote path is live) and seq must be
	// frozen (empty rounds do not commit blocks).
	for i := 0; i < 4; i++ {
		resp, err := c.ctrl[i].GetStatus(ctx, &pb.GetStatusRequest{})
		if err != nil {
			return fmt.Errorf("GetStatus node%d during drop: %w", i, err)
		}
		if resp.Seq != before.Seq {
			return fmt.Errorf("unexpected block commit with votes dropped: node%d seq=%d (expected %d)",
				i, resp.Seq, before.Seq)
		}
		if resp.Round <= before.Round {
			return fmt.Errorf("empty-vote path stalled: node%d round=%d (expected > %d)",
				i, resp.Round, before.Round)
		}
	}
	fmt.Printf("empty-vote path verified: seq frozen at %d, rounds advanced beyond %d\n", before.Seq, before.Round)

	// Clear filter — real voting resumes and blocks commit.
	for i := 0; i < 4; i++ {
		if err := c.ClearMessages(i); err != nil {
			return err
		}
	}

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-after-drop-votes")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(before.Seq+3, 60*time.Second); err != nil {
		return fmt.Errorf("cluster failed to commit blocks after clearing vote filter: %w", err)
	}

	return c.verifyAgreement()
}

// testDropReplicationResponse drops all incoming replication_response on node3,
// then partitions it to create a block gap. After healing the partition node3
// attempts to replicate but all responses are dropped — it stays behind.
// Clearing the filter lets node3 catch up and rejoin.
func testDropReplicationResponse(c *Cluster) error {
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-before-drop-replication")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(4, 60*time.Second); err != nil {
		return err
	}

	// Install the filter before partitioning so node3 can't replicate on heal.
	if err := c.DropMessages(3, "replication_response"); err != nil {
		return err
	}

	if err := c.Partition(3, 0, 1, 2); err != nil {
		return fmt.Errorf("partition node3: %w", err)
	}

	gapStart, err := c.ctrl[3].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus node3 before gap: %w", err)
	}

	for i := 0; i < 3; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-gap")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocksOnNodes(gapStart.Seq+5, 30*time.Second, 0, 1, 2); err != nil {
		return fmt.Errorf("nodes 0-2 failed to build gap: %w", err)
	}

	// Heal the partition — node3 sees messages again but replication_response is dropped.
	if err := c.Heal(3); err != nil {
		return fmt.Errorf("heal node3: %w", err)
	}

	// Give node3 time to attempt replication (which will fail silently).
	// 2s is enough to verify it is blocked; a longer sleep risks nodes 0-2
	// advancing thousands of blocks (BlockBuilder never clears pending).
	time.Sleep(2 * time.Second)

	stuck, err := c.ctrl[3].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus node3 while filter active: %w", err)
	}
	if stuck.Seq >= gapStart.Seq+5 {
		return fmt.Errorf("expected node3 to be stuck behind seq=%d but it reached seq=%d",
			gapStart.Seq+5, stuck.Seq)
	}
	fmt.Printf("replication blocked: node3 at seq=%d while peers are at seq>=%d\n",
		stuck.Seq, gapStart.Seq+5)

	// Snapshot the current head of nodes 0-2 before clearing — this is the
	// target node3 must replicate up to.
	head, err := c.ctrl[0].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus node0 before clear: %w", err)
	}

	// Clear filter — replication succeeds.
	if err := c.ClearMessages(3); err != nil {
		return err
	}

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-after-drop-replication")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	// Wait for all nodes to reach head+3. Nodes 0-2 are already past head;
	// node3 must replicate the gap first. Use a generous timeout since the
	// gap can be large.
	if err := c.waitForBlocks(head.Seq+3, 120*time.Second); err != nil {
		return fmt.Errorf("cluster failed to converge after clearing replication filter: %w", err)
	}

	return c.verifyAgreement()
}

// testDropBlockMessage drops all incoming block_message on node3 so it never
// receives proposals from other leaders. Node3 always empty-votes when it is
// not the leader, while the other 3 nodes vote normally. This exercises the
// mixed-quorum path: rounds led by non-node3 leaders commit via 3 real votes
// (0,1,2); rounds led by node3 commit normally since node3 broadcasts its own
// proposal. After clearing the filter all 4 nodes must agree.
func testDropBlockMessage(c *Cluster) error {
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-before-drop-block")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(4, 60*time.Second); err != nil {
		return err
	}

	before, err := c.ctrl[0].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus before drop: %w", err)
	}

	if err := c.DropMessages(3, "block_message"); err != nil {
		return err
	}

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-during-drop-block")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}

	// Cluster must advance: rounds where node3 is not leader commit via votes
	// from nodes 0,1,2 (quorum=3); node3 empty-votes those rounds.
	// Rounds where node3 is leader commit normally (node3 broadcasts the block,
	// it doesn't need to receive it). Allow extra time for empty-vote rounds.
	if err := c.waitForBlocks(before.Seq+6, 90*time.Second); err != nil {
		return fmt.Errorf("cluster failed to advance with block_message dropped on node3: %w", err)
	}
	fmt.Println("mixed empty/real vote path verified")

	if err := c.ClearMessages(3); err != nil {
		return err
	}

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-after-drop-block")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(before.Seq+8, 60*time.Second); err != nil {
		return fmt.Errorf("cluster failed to converge after clearing block filter: %w", err)
	}

	return c.verifyAgreement()
}

// testDropFinalizeVote drops all incoming finalize_vote on every node.
// Notarization QCs form normally (votes are not dropped) so rounds advance,
// but no finalization QC can ever form — seq stays frozen.
// After clearing the filter finalization catches up and blocks are committed.
func testDropFinalizeVote(c *Cluster) error {
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-before-drop-finalize")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(4, 60*time.Second); err != nil {
		return err
	}

	// Drop finalize_vote on all nodes — notarization still forms, finalization can't.
	for i := 0; i < 4; i++ {
		if err := c.DropMessages(i, "finalize_vote"); err != nil {
			return err
		}
	}

	// Settle: let any in-flight finalize votes resolve before snapshotting.
	time.Sleep(3 * time.Second)

	before, err := c.ctrl[0].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus after filter applied: %w", err)
	}

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-during-drop-finalize")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}

	// Wait for several rounds — rounds should advance via notarization, seq must stay frozen.
	// Stay within MaxRoundWindow (10) to avoid vote rejection.
	time.Sleep(11 * time.Second)

	for i := 0; i < 4; i++ {
		resp, err := c.ctrl[i].GetStatus(ctx, &pb.GetStatusRequest{})
		if err != nil {
			return fmt.Errorf("GetStatus node%d during drop: %w", i, err)
		}
		if resp.Seq != before.Seq {
			return fmt.Errorf("unexpected commit with finalize_vote dropped: node%d seq=%d (expected %d)",
				i, resp.Seq, before.Seq)
		}
		if resp.Round <= before.Round {
			return fmt.Errorf("rounds stalled with finalize_vote dropped: node%d round=%d (expected > %d)",
				i, resp.Round, before.Round)
		}
	}
	fmt.Printf("finalize_vote drop verified: seq frozen at %d, rounds advanced beyond %d\n", before.Seq, before.Round)

	// Clear filter — finalization QC forms, seq catches up.
	for i := 0; i < 4; i++ {
		if err := c.ClearMessages(i); err != nil {
			return err
		}
	}

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-after-drop-finalize")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(before.Seq+3, 60*time.Second); err != nil {
		return fmt.Errorf("cluster failed to commit blocks after clearing finalize_vote filter: %w", err)
	}

	return c.verifyAgreement()
}

// testDropEmptyVote drops all incoming empty_vote_message on every node.
// The empty-vote path (timeout → EmptyNotarization → round++) is blocked, but
// the commit path (vote → notarization → finalize → commit) is unaffected.
// With transactions available the cluster must still make progress via real votes.
func testDropEmptyVote(c *Cluster) error {
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-before-drop-empty-vote")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(4, 60*time.Second); err != nil {
		return err
	}

	before, err := c.ctrl[0].GetStatus(ctx, &pb.GetStatusRequest{})
	if err != nil {
		return fmt.Errorf("GetStatus before drop: %w", err)
	}

	// Drop empty_vote_message on all nodes.
	for i := 0; i < 4; i++ {
		if err := c.DropMessages(i, "empty_vote_message"); err != nil {
			return err
		}
	}

	// Submit tx to all nodes — commit path (real votes) must still work.
	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-during-drop-empty-vote")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}

	// With txs available the cluster commits via real votes despite empty_vote being dropped.
	if err := c.waitForBlocks(before.Seq+3, 30*time.Second); err != nil {
		return fmt.Errorf("commit path broken with empty_vote dropped: %w", err)
	}
	fmt.Println("commit path works independently of empty-vote path")

	// Clear filter and verify full agreement.
	for i := 0; i < 4; i++ {
		if err := c.ClearMessages(i); err != nil {
			return err
		}
	}

	for i := 0; i < 4; i++ {
		_, err := c.ctrl[i].SubmitTransaction(ctx, &pb.SubmitTransactionRequest{Data: []byte("tx-after-drop-empty-vote")})
		if err != nil {
			return fmt.Errorf("SubmitTransaction to node%d: %w", i, err)
		}
	}
	if err := c.waitForBlocks(before.Seq+5, 60*time.Second); err != nil {
		return fmt.Errorf("cluster failed to converge after clearing empty_vote filter: %w", err)
	}

	return c.verifyAgreement()
}

