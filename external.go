// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"context"

	"github.com/ava-labs/simplex/common"
	metadata "github.com/ava-labs/simplex/msm"
)

type RawBlock struct {
	Metadata        metadata.StateMachineMetadata `canoto:"value,1"`
	InnerBlockBytes []byte                        `canoto:"bytes,2"`

	canotoData canotoData_RawBlock
}

type ParsedBlock struct {
	metadata.StateMachineBlock
	msm *metadata.StateMachine
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
	rawBlock := &RawBlock{
		Metadata:        p.Metadata,
		InnerBlockBytes: innerBlockBytes,
	}
	return rawBlock.MarshalCanoto(), nil
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

func (p *ParsedBlock) SealingBlockInfo() *common.SealingBlockInfo {
	if p.Metadata.SimplexEpochInfo.BlockValidationDescriptor == nil {
		return nil
	}

	bdc := p.Metadata.SimplexEpochInfo.BlockValidationDescriptor
	var nodes common.Nodes

	for _, vdr := range bdc.AggregatedMembership.Members {
		nodes = append(nodes, common.Node{
			Id:     vdr.NodeID[:],
			Weight: vdr.Weight,
			PK:     vdr.BLSKey,
		})
	}

	return &common.SealingBlockInfo{
		ValidatorSet: nodes,
	}
}
