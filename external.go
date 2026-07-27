// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"context"
	"sync"

	"github.com/ava-labs/simplex/common"
	metadata "github.com/ava-labs/simplex/msm"
)

type ParsedBlock struct {
	metadata.StateMachineBlock
	msm *metadata.StateMachine
	// sizeLock locks size, so Size() can be invoked concurrently
	sizeLock sync.Mutex
	// size caches the length of the Bytes encoding, computed on first use
	size int
}

func (p *ParsedBlock) BlockHeader() common.BlockHeader {
	var md *common.ProtocolMetadata
	var err error
	if len(p.Metadata.SimplexProtocolMetadata) > 0 {
		md, err = common.ProtocolMetadataFromBytes(p.Metadata.SimplexProtocolMetadata)
		if err != nil {
			panic(err) // TODO: handle error
		}
	} else {
		md = &common.ProtocolMetadata{}
	}

	digest := p.StateMachineBlock.Digest()
	return common.BlockHeader{
		ProtocolMetadata: *md,
		Digest:           digest,
	}
}

func (p *ParsedBlock) Blacklist() common.Blacklist {
	var blacklist common.Blacklist
	_ = blacklist.FromBytes(p.Metadata.SimplexBlacklist) // TODO: encode blacklist with Canoto
	return blacklist
}

func (p *ParsedBlock) Verify(ctx context.Context) (common.VerifiedBlock, error) {
	if err := p.msm.VerifyBlock(ctx, &p.StateMachineBlock); err != nil {
		return nil, err
	}
	return p, nil
}
func (p *ParsedBlock) Size() int {
	p.sizeLock.Lock()
	defer p.sizeLock.Unlock()
	if p.size == 0 {
		bytes, err := p.Bytes()
		if err != nil {
			return 0
		}
		p.size = len(bytes)
	}
	return p.size
}
