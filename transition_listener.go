package simplex

import (
	"errors"
	"time"

	"github.com/ava-labs/simplex/avalanchego"
	"github.com/ava-labs/simplex/common"

	metadata "github.com/ava-labs/simplex/msm"
)

// epochTransitionListener reacts to blocks committed to storage. When a
// transition block is indexed, it performs any tasks required of this node to
// complete the epoch transition, such as sending out approval messages.
// Non-validators should also use this listener, since they may become
// validators after the transition.
type epochTransitionListener struct {
	// broadcaster is used for broadcasting potential approvals and a auxiliary information
	// it should be broadcasted to the validators of the current epoch
	broadcaster Broadcaster

	myNodeID avalanchego.NodeID

	// onEpochChange is a callback the listener invokes once a sealing block for `epoch` has been indexed.
	onEpochChange func(epoch uint64, validators common.Nodes) error

	logger common.Logger
}

func newEpochTransitionListener(logger common.Logger, broadcaster Broadcaster, myNodeID avalanchego.NodeID, onEpochChange func(epoch uint64, validator common.Nodes) error) *epochTransitionListener {
	return &epochTransitionListener{
		broadcaster:   broadcaster,
		myNodeID:      myNodeID,
		onEpochChange: onEpochChange,
		logger:        logger,
	}
}

func (a *epochTransitionListener) onIndex(block *ParsedBlock) error {
	switch block.Type() {
	case metadata.BlockTypeSealing:
		if block.SealingBlockInfo() == nil {
			return errors.New("sealing block has empty SealingBlockInfo")
		}
		return a.onEpochChange(block.BlockHeader().Seq, block.SealingBlockInfo().ValidatorSet)
	case metadata.BlockTypeTransitioning:
		return a.handleTransitionBlock(block)
	}

	return nil
}

func (a *epochTransitionListener) handleTransitionBlock(block *ParsedBlock) error {
	nextEpochPChainReference := block.Metadata.SimplexEpochInfo.NextPChainReferenceHeight

	// if our node is not in the next validator set, no need to send anything.
	nextEpochValidatorSet, err := block.msm.GetValidatorSet(nextEpochPChainReference)
	if err != nil {
		return err
	}

	indexes := nextEpochValidatorSet.IndexByNodeID()
	if _, ok := indexes[a.myNodeID]; !ok {
		return nil // we are not in the next validator set
	}

	auxInfoHistory, err := metadata.GetAuxiliaryHistory(&block.StateMachineBlock, block.BlockHeader().Seq, block.msm.GetBlock, block.msm.AuxiliaryInfoApp.DefaultVersionID())
	isSufficient, err := block.msm.AuxiliaryInfoApp.IsSufficient(auxInfoHistory.OldestVersionID, nextEpochValidatorSet, auxInfoHistory.Data)

	if isSufficient {
		// no more auxiliary info to send, maybe send our approval
		lastAuxInfoDigest := auxInfoHistory.LastHistoryDigest()
		return a.maybeSendApprovals(block, lastAuxInfoDigest)
	}

	// we need more auxiliary information, attempt to generate
	generatedAuxInfo, err := block.msm.AuxiliaryInfoApp.Generate(auxInfoHistory.OldestVersionID, nextEpochValidatorSet, auxInfoHistory.Data)
	if err != nil {
		return err
	}

	if generatedAuxInfo == nil {
		return nil
	}

	auxInfoMessage := &common.Message{
		AuxiliaryInfo: &common.AuxiliaryInfo{
			Version: auxInfoHistory.OldestVersionID,
			Data:    generatedAuxInfo,
		},
	}

	a.broadcaster.Broadcast(auxInfoMessage)
	return nil
}

// TODO: use common.Digest
func (a *epochTransitionListener) maybeSendApprovals(block *ParsedBlock, auxInfoDigest [32]byte) error {
	nextEpochPChainReference := block.Metadata.SimplexEpochInfo.NextPChainReferenceHeight

	sig, err := metadata.SignApproval(block.msm.Signer, nextEpochPChainReference, auxInfoDigest)
	if err != nil {
		return err
	}

	approval := common.ValidatorSetApproval{
		NodeID:        a.myNodeID,
		PChainHeight:  nextEpochPChainReference,
		AuxInfoDigest: auxInfoDigest,
		Signature:     sig,
	}

	approvalMessage := common.Message{
		EpochTransitionApproval: &common.EpochTransitionApproval{
			Approval: approval,
		},
	}

	a.broadcaster.Broadcast(&approvalMessage)
	timestamp := uint64(time.Now().UnixMilli())
	return block.msm.HandleApproval(&approval, timestamp)
}
