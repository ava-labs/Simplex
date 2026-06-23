// comm provides the gRPC Communication implementation that
// satisfies both common.Communication and the
// NodeService/ControlService gRPC server interfaces.
package comm

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ava-labs/simplex/common"
	pb "github.com/ava-labs/simplex/jepsen/node/proto"
)

// MessageHandler is called whenever a SimplexMessage arrives for the local
// engine (via the NodeService.SendMessage RPC).
type MessageHandler interface {
	HandleMessage(msg *common.Message, from common.NodeID) error
}

// Comm implements common.Communication and both gRPC server interfaces.
type Comm struct {
	pb.UnimplementedNodeServiceServer
	pb.UnimplementedControlServiceServer

	selfID  common.NodeID
	nodes   common.Nodes   // all nodes including self
	clients map[string]pb.NodeServiceClient

	handler    MessageHandler
	bb         BlockBuilderIface
	statusFunc func() (epoch, round, seq uint64)
	storage    StorageIface

	bd common.BlockDeserializer
	qd common.QCDeserializer

	log *zap.Logger

	mu      sync.RWMutex
	started atomic.Bool
}

// BlockBuilderIface is a subset of BlockBuilder used by Comm.
type BlockBuilderIface interface {
	SubmitTransaction(data []byte)
}

// StorageIface is a subset of common.Storage used by GetCommittedBlocks.
type StorageIface interface {
	NumBlocks() uint64
	Retrieve(seq uint64) (common.VerifiedBlock, common.Finalization, error)
}

// New creates a Comm, pre-dialing all peer addresses.
// peerAddrs maps NodeID (hex string of compressed pubkey) → "host:port".
func New(
	selfID common.NodeID,
	nodes common.Nodes,
	peerAddrs map[string]string,
	handler MessageHandler,
	bb BlockBuilderIface,
	statusFunc func() (epoch, round, seq uint64),
	storage StorageIface,
	bd common.BlockDeserializer,
	qd common.QCDeserializer,
	log *zap.Logger,
) (*Comm, error) {
	clients := make(map[string]pb.NodeServiceClient, len(nodes))
	for _, n := range nodes {
		nodeStr := string(n.Node)
		if n.Node.Equals(selfID) {
			continue
		}
		addr, ok := peerAddrs[fmt.Sprintf("%x", []byte(n.Node))]
		if !ok {
			return nil, fmt.Errorf("comm: no address for node %x", []byte(n.Node))
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("comm: dial %s: %w", addr, err)
		}
		clients[nodeStr] = pb.NewNodeServiceClient(conn)
	}

	return &Comm{
		selfID:     selfID,
		nodes:      nodes,
		clients:    clients,
		handler:    handler,
		bb:         bb,
		statusFunc: statusFunc,
		storage:    storage,
		bd:         bd,
		qd:         qd,
		log:        log,
	}, nil
}

// common.Communication implementation

// Nodes returns all participants (including self), each with weight 1.
func (c *Comm) Nodes() common.Nodes {
	return c.nodes
}

// Send sends a message to a single destination node.
func (c *Comm) Send(msg *common.Message, destination common.NodeID) {
	c.mu.RLock()
	client, ok := c.clients[string(destination)]
	c.mu.RUnlock()
	if !ok {
		c.log.Warn("comm: unknown destination", zap.String("node", fmt.Sprintf("%x", []byte(destination))))
		return
	}

	pbMsg, err := toProto(msg)
	if err != nil {
		c.log.Error("comm: toProto", zap.Error(err))
		return
	}

	go func() {
		_, err := client.SendMessage(context.Background(), &pb.SendMessageRequest{
			From:    c.selfID,
			Message: pbMsg,
		})
		if err != nil {
			c.log.Warn("comm: SendMessage RPC failed",
				zap.String("dst", fmt.Sprintf("%x", []byte(destination))),
				zap.Error(err))
		}
	}()
}

// Broadcast sends the message to all nodes except self.
func (c *Comm) Broadcast(msg *common.Message) {
	pbMsg, err := toProto(msg)
	if err != nil {
		c.log.Error("comm: toProto in Broadcast", zap.Error(err))
		return
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	for nodeStr, client := range c.clients {
		nodeStr := nodeStr
		client := client
		go func() {
			_, err := client.SendMessage(context.Background(), &pb.SendMessageRequest{
				From:    c.selfID,
				Message: pbMsg,
			})
			if err != nil {
				c.log.Warn("comm: Broadcast RPC failed",
					zap.String("dst", fmt.Sprintf("%x", []byte(nodeStr))),
					zap.Error(err))
			}
		}()
	}
}

// NodeService gRPC server

// SendMessage is the gRPC handler for incoming protocol messages from peers.
// It dispatches to the engine handler in a goroutine to avoid deadlocks.
func (c *Comm) SendMessage(_ context.Context, req *pb.SendMessageRequest) (*pb.SendMessageResponse, error) {
	msg, err := fromProto(req.Message, c.bd, c.qd)
	if err != nil {
		return nil, fmt.Errorf("comm: fromProto: %w", err)
	}
	from := common.NodeID(req.From)

	go func() {
		if err := c.handler.HandleMessage(msg, from); err != nil {
			c.log.Warn("comm: HandleMessage error", zap.Error(err))
		}
	}()

	return &pb.SendMessageResponse{}, nil
}

// ControlService gRPC server

// SubmitTransaction forwards a client transaction to the BlockBuilder.
func (c *Comm) SubmitTransaction(_ context.Context, req *pb.SubmitTransactionRequest) (*pb.SubmitTransactionResponse, error) {
	c.bb.SubmitTransaction(req.Data)
	return &pb.SubmitTransactionResponse{Accepted: true}, nil
}

// GetStatus returns the current epoch, round and committed block count.
func (c *Comm) GetStatus(_ context.Context, _ *pb.GetStatusRequest) (*pb.GetStatusResponse, error) {
	epoch, round, seq := c.statusFunc()
	return &pb.GetStatusResponse{
		Epoch: epoch,
		Round: round,
		Seq:   seq,
	}, nil
}

// GetCommittedBlocks returns committed block headers starting from req.FromSeq.
func (c *Comm) GetCommittedBlocks(_ context.Context, req *pb.GetCommittedBlocksRequest) (*pb.GetCommittedBlocksResponse, error) {
	total := c.storage.NumBlocks()
	var blocks []*pb.CommittedBlock
	for seq := req.FromSeq; seq < total; seq++ {
		vb, _, err := c.storage.Retrieve(seq)
		if err != nil {
			break
		}
		bh := vb.BlockHeader()
		blocks = append(blocks, &pb.CommittedBlock{
			Seq:    bh.Seq,
			Round:  bh.Round,
			Epoch:  bh.Epoch,
			Digest: bh.Digest[:],
		})
	}
	return &pb.GetCommittedBlocksResponse{Blocks: blocks}, nil
}
