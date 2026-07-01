// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"context"
	"time"

	"github.com/ava-labs/simplex/common"
	metadata "github.com/ava-labs/simplex/msm"
	"github.com/ava-labs/simplex/wal"
)

type ParameterConfig struct {
	// WalMaxEntryCount is the maximum number of entries in the write-ahead log before it is closed.
	WALMaxEntryCount int
	// MaxnetworkDelay is the assumed upper bound on the network delay for messages to be delivered.
	MaxNetworkDelay time.Duration
	// MaxRoundWindow is the maximum number of rounds that can be stored in memory.
	MaxRoundWindow uint64
}

// PlatformChain is an interface that abstracts the interaction with the P-chain.
type PlatformChain interface {
	GetValidatorSet(uint64) (metadata.NodeBLSMappings, error)
	// GenesisValidatorSet returns the first ever validator set for this network.
	GenesisValidatorSet() metadata.NodeBLSMappings
	// GetMinimumHeight returns the minimum height of the block still in the proposal window.
	GetMinimumHeight(context.Context) (uint64, error)
	// GetCurrentHeight returns the current height of the P-chain.
	GetCurrentHeight(context.Context) (uint64, error)
	// WaitForProgress should block until either the context is cancelled, or the P-chain height has increased from the provided pChainHeight.
	WaitForProgress(ctx context.Context, pChainHeight uint64) error
	// LastNonSimplexBlockPChainHeight returns the P-chain height of the last non-simplex block in the chain.
	LastNonSimplexBlockPChainHeight() uint64
}

// Sender is an interface that defines the ability to send messages to other nodes in the network.
type Sender interface {
	// Send sends a message to the given destination node
	Send(msg *common.Message, destination common.NodeID)
}

type VM interface {
	// BuildBlock builds a block given the current context and the P-chain height.
	BuildBlock(ctx context.Context, pChainHeight uint64) (metadata.VMBlock, error)

	// WaitForPendingBlock returns when either the given context is cancelled,
	// or when the VM signals that a block should be built.
	WaitForPendingBlock(ctx context.Context)

	// ParseBlock parses the given block bytes into a VMBlock.
	ParseBlock(context.Context, []byte) (metadata.VMBlock, error)

	// ComputeICMEpoch computes the ICM epoch transition given the input parameters.
	ComputeICMEpoch(input metadata.ICMEpochInput) metadata.ICMEpochInfo
}

type Storage interface {
	// GetBlock retrieves the block and finalization at [seq] with the given digest.
	// If the digest is nil, the block with the given sequence number is returned.
	// If [seq] the block cannot be found, returns ErrBlockNotFound.
	GetBlock(seq uint64) (metadata.StateMachineBlock, *common.Finalization, error)

	// NumBlocks returns the number of blocks stored in the storage.
	NumBlocks() uint64

	// Index indexes the given block and finalization in the storage.
	Index(ctx context.Context, block common.VerifiedBlock, certificate common.Finalization) error

	// CreateWAL creates a new Write-Ahead Log (WAL).
	CreateWAL() (wal.DeletableWAL, error)
}

type CryptoOps interface {
	Sign(message []byte) ([]byte, error)
	AggregateKeys(keys ...[]byte) ([]byte, error)
	VerifySignature(message []byte, signature []byte, publicKey []byte) error
	CreateSignatureAggregator([]common.Node) common.SignatureAggregator
	DeserializeQuorumCertificate(bytes []byte) (common.QuorumCertificate, error)
}

type MSMConfig struct {
	LastNonSimplexInnerBlock metadata.VMBlock
	ComputeICMEpoch          metadata.ICMEpochTransition
}

type EpochParams struct {
	ID                         common.NodeID
	Sender                     Sender
	Storage                    Storage
	QCDeserializer             common.QCDeserializer
	BlockDeserializer          common.BlockDeserializer
	Verifier                   common.SignatureVerifier
	MaxProposalWait            time.Duration
	MaxRoundWindow             uint64
	MaxRebroadcastWait         time.Duration
	FinalizeRebroadcastTimeout time.Duration
	MaxWALSize                 int
	WALCreator                 wal.Creator
	WALs                       []wal.DeletableWAL
}
