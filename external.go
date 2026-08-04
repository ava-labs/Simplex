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

	// lock guards size, so Size() can be invoked concurrently
	lock sync.Mutex
	// size caches the length of the Bytes encoding, computed on first use
	size int
}

func (p *ParsedBlock) Bytes() ([]byte, error) {
	var innerBlockBytes []byte
	if p.InnerBlock != nil {
		rawInnerBlock, err := p.InnerBlock.Bytes()
		if err != nil {
			return nil, err
		}
		innerBlockBytes = rawInnerBlock
	}
	rawBlock := &metadata.RawBlock{
		Metadata:        p.Metadata.Clone(),
		InnerBlockBytes: innerBlockBytes,
	}
	return rawBlock.MarshalCanoto(), nil
}

func (p *ParsedBlock) BlockHeader() common.BlockHeader {
	md := p.Metadata.SimplexProtocolMetadata.Clone()
	digest := p.StateMachineBlock.Digest()
	return common.BlockHeader{
		ProtocolMetadata: md,
		Digest:           digest,
	}
}

func (p *ParsedBlock) Blacklist() common.Blacklist {
	return p.Metadata.SimplexBlacklist.Clone()
}

func (p *ParsedBlock) Verify(ctx context.Context) (common.VerifiedBlock, error) {
	if err := p.msm.VerifyBlock(ctx, &p.StateMachineBlock); err != nil {
		return nil, err
	}
	return p, nil
}
func (p *ParsedBlock) Size() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.size == 0 {
		bytes := p.Bytes()
		p.size = len(bytes)
	}
	return p.size
}
