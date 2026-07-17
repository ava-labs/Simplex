package simplex

import (
	"errors"
	"fmt"

	"github.com/ava-labs/simplex/avalanchego"
	"github.com/ava-labs/simplex/common"

	metadata "github.com/ava-labs/simplex/msm"
)

// EpochTransitionListener listens to finalizations that not an epoch change is incoming.
// If the pChain height referenced includes our node ID we send and broadcast approvals
// our own approval until the epoch change is complete.
// This is better than the previous where we would need to wait for validators to send there approval
// only when they are block builders.
// It also makes the logic the same for validators and non-validators to reduce code duplication.
// It also means that we can process and send approvals asynchronously from block building, speeding up the process for epoch transitions.
type EpochTransitionListener struct {
	// to send approvals
	comm common.Communication

	// to resend in the case of network issues
	// the id is the epoch number we are waiting
	timeouts common.TimeoutHandler[approvalTask]

	myNodeID avalanchego.NodeID

	onEpochChange func(epoch uint64)
}

func NewEpochTransitionListener(comm common.Communication, myNodeID avalanchego.NodeID, onEpochChange func(epoch uint64)) *EpochTransitionListener {
	return &EpochTransitionListener{
		comm:          comm,
		myNodeID:      myNodeID,
		onEpochChange: onEpochChange,
		// TODO: timeouts: common.NewTimeoutHandler(),
	}
}

func (a *EpochTransitionListener) createSelfApproval(signer common.Signer, nextPChainReferenceHeight uint64, auxInfoDigest [32]byte) ([]byte, error) {
	toBeSigned, err := metadata.AssembleApprovalToBeSigned(nextPChainReferenceHeight, auxInfoDigest)
	if err != nil {
		return nil, err
	}

	sig, err := signer.Sign(toBeSigned)
	if err != nil {
		return nil, fmt.Errorf("failed to sign approval: %w", err)
	}
	return sig, nil
}

func (a *EpochTransitionListener) onIndex(block *ParsedBlock) error {
	switch block.Type() {
	case metadata.BlockTypeSealing:
		return a.handleSealingBlockIndexed(block)
	case metadata.BlockTypeTransitioning:
		return a.handleTransitionBlock(block)
	}

	return nil
}

// handleSealingBlockIndexed removes all timeout tasks <= oldEpoch
func (a *EpochTransitionListener) handleSealingBlockIndexed(block *ParsedBlock) error {
	oldEpoch := block.Metadata.SimplexEpochInfo.EpochNumber
	a.timeouts.RemoveOldTasks(
		func(id approvalTask, _ struct{}) bool {
			return id.epoch <= oldEpoch
		},
	)

	sealingInfo := block.SealingBlockInfo()
	if sealingInfo == nil {
		return errors.New("sealing block does not have sealingInfo")
	}

	a.onEpochChange(block.BlockHeader().Seq)
	return nil
}

func (a *EpochTransitionListener) handleTransitionBlock(block *ParsedBlock) error {
	nextEpochPChainReference := block.Metadata.SimplexEpochInfo.NextPChainReferenceHeight

	// is our node a validator in the next epoch?
	nextEpochValidatorSet, err := block.msm.GetValidatorSet(nextEpochPChainReference)
	if err != nil {
		return err
	}

	indexes := nextEpochValidatorSet.IndexByNodeID()
	if _, ok := indexes[a.myNodeID]; !ok {
		return nil // we are not in the next validator set, return
	}

	auxInfoHistory, versionID, err := metadata.CollectAuxiliaryInfo(&block.StateMachineBlock, block.BlockHeader().Seq, block.msm.GetBlock, block.msm.AuxiliaryInfoApp.DefaultVersionID())
	isSufficient, err := block.msm.AuxiliaryInfoApp.IsSufficient(versionID, nextEpochValidatorSet, auxInfoHistory.Data)

	if isSufficient {
		// maybe handle approvals
		lastAuxInfoDigest := auxInfoHistory.LastHistoryDigest()
		return a.maybeSendApprovals(block, lastAuxInfoDigest)
	}

	generatedAuxInfo, err := block.msm.AuxiliaryInfoApp.Generate(versionID, nextEpochValidatorSet, auxInfoHistory.Data)
	if err != nil {
		return err
	}

	if generatedAuxInfo == nil {
		return nil
	}

	auxInfoMessage := &common.Message{
		AuxiliaryInfo: &common.AuxiliaryInfo{
			Version: versionID,
			Data:    generatedAuxInfo,
		},
	}

	a.comm.Broadcast(auxInfoMessage)
	return nil
}

type approvalTask struct {
	epoch         uint64
	pChainRef     uint64
	auxInfoDigest [32]byte
}

// TODO: use common.Digest
// TODO: if the digest has changed do we need to resend our approval?
// TODO: can the digest ever change?
func (a *EpochTransitionListener) maybeSendApprovals(block *ParsedBlock, auxInfoDigest [32]byte) error {
	nextEpochPChainReference := block.Metadata.SimplexEpochInfo.NextPChainReferenceHeight
	epoch := block.Metadata.SimplexEpochInfo.EpochNumber

	task := approvalTask{
		epoch:         epoch,
		auxInfoDigest: auxInfoDigest,
		pChainRef:     nextEpochPChainReference,
	}

	// we have recently already sent this approval, the timeout handler will re-send if necessary
	if a.timeouts.Has(task) {
		return nil
	}

	sig, err := a.createSelfApproval(block.msm.Signer, nextEpochPChainReference, auxInfoDigest)
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

	a.comm.Broadcast(&approvalMessage)
	// TODO: timestamp
	block.msm.HandleApproval(&approval, 0)
	a.timeouts.AddTask(task)
	return nil
}
