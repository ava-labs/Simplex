package simplex

import (
	"testing"

	"github.com/ava-labs/simplex/avalanchego"
	"github.com/ava-labs/simplex/common"
	metadata "github.com/ava-labs/simplex/msm"
	"github.com/ava-labs/simplex/testutil"

	"github.com/stretchr/testify/require"
)

var testNodeID = avalanchego.NodeID{1}

type recordingBroadcaster struct {
	messages []*common.Message
}

func (rb *recordingBroadcaster) Broadcast(msg *common.Message) {
	rb.messages = append(rb.messages, msg)
}

type stubSigner struct {
	sig []byte
}

func (s stubSigner) Sign([]byte) ([]byte, error) {
	return s.sig, nil
}

type stubSignatureVerifier struct{}

func (stubSignatureVerifier) VerifySignature([]byte, []byte, []byte) error {
	return nil
}

type stubAuxInfoApp struct {
	sufficient bool
	generated  []byte
}

func (s *stubAuxInfoApp) IsLegalAppend(common.VersionID, metadata.NodeBLSMappings, [][]byte, []byte) error {
	return nil
}

func (s *stubAuxInfoApp) IsSufficient(common.VersionID, metadata.NodeBLSMappings, [][]byte) (bool, error) {
	return s.sufficient, nil
}

func (s *stubAuxInfoApp) Generate(common.VersionID, metadata.NodeBLSMappings, [][]byte) ([]byte, error) {
	return s.generated, nil
}

func (s *stubAuxInfoApp) DefaultVersionID() common.VersionID {
	return 7
}

type listenerTestEnv struct {
	broadcaster *recordingBroadcaster
	epochs      []epochChange
	listener    *epochTransitionListener
}

func newListenerTestEnv(t *testing.T) *listenerTestEnv {
	env := &listenerTestEnv{broadcaster: &recordingBroadcaster{}}
	env.listener = newEpochTransitionListener(testutil.MakeLogger(t, 1), env.broadcaster, testNodeID, func(epoch uint64, validators common.Nodes) error {
		env.epochs = append(env.epochs, epochChange{
			epoch:      epoch,
			validators: validators,
		})
		return nil
	})
	return env
}

// newTransitionBlock returns a ParsedBlock of type BlockTypeTransitioning
// backed by a StateMachine with the given validator set and auxiliary info app.
func newTransitionBlock(t *testing.T, nextPChainRef uint64, validatorSet metadata.NodeBLSMappings, auxApp metadata.AuxiliaryInfoGenVerifier) *ParsedBlock {
	block := &ParsedBlock{
		StateMachineBlock: metadata.StateMachineBlock{
			Metadata: metadata.StateMachineMetadata{
				SimplexProtocolMetadata: (&common.ProtocolMetadata{Seq: 10}).Bytes(),
				SimplexEpochInfo: metadata.SimplexEpochInfo{
					NextPChainReferenceHeight: nextPChainRef,
				},
			},
		},
		msm: &metadata.StateMachine{
			Config: &metadata.Config{
				Logger: testutil.MakeLogger(t, 1),
				Signer: stubSigner{sig: []byte("signature")},
				GetValidatorSet: func(pChainHeight uint64) (metadata.NodeBLSMappings, error) {
					require.Equal(t, nextPChainRef, pChainHeight)
					return validatorSet, nil
				},
				AuxiliaryInfoApp:  auxApp,
				SignatureVerifier: stubSignatureVerifier{},
			},
		},
	}
	require.Equal(t, metadata.BlockTypeTransitioning, block.Type())
	return block
}

func TestSealingBlockCallback(t *testing.T) {
	const sealingSeq = uint64(42)

	env := newListenerTestEnv(t)

	validatorSet := metadata.NodeBLSMappings{{NodeID: testNodeID, BLSKey: []byte("bls-key"), Weight: 5}}
	block := &ParsedBlock{
		StateMachineBlock: metadata.StateMachineBlock{
			Metadata: metadata.StateMachineMetadata{
				SimplexProtocolMetadata: (&common.ProtocolMetadata{Seq: sealingSeq}).Bytes(),
				SimplexEpochInfo: metadata.SimplexEpochInfo{
					// a non-empty PrevSealingBlockHash distinguishes a sealing block from the zero block
					PrevSealingBlockHash: [32]byte{1},
					BlockValidationDescriptor: &metadata.BlockValidationDescriptor{
						AggregatedMembership: metadata.AggregatedMembership{Members: validatorSet},
					},
				},
			},
		},
	}
	require.Equal(t, metadata.BlockTypeSealing, block.Type())

	require.NoError(t, env.listener.onIndex(block))
	require.Empty(t, env.broadcaster.messages)

	// the callback should have been invoked with the sealing block's epoch and validator set
	expectedValidators := common.Nodes{{Id: testNodeID[:], Weight: 5, PK: []byte("bls-key")}}
	require.Equal(t, []epochChange{{epoch: sealingSeq, validators: expectedValidators}}, env.epochs)
}

func TestTransitionNotInValidatorSet(t *testing.T) {
	env := newListenerTestEnv(t)

	// the next validator set does not contain our node
	otherValidator := metadata.NodeBLSMappings{{NodeID: avalanchego.NodeID{2}, Weight: 1}}
	auxApp := &stubAuxInfoApp{sufficient: false, generated: []byte("more aux info")}
	block := newTransitionBlock(t, 100, otherValidator, auxApp)

	require.NoError(t, env.listener.onIndex(block))
	require.Empty(t, env.broadcaster.messages)
	require.Empty(t, env.epochs)
}

func TestTransitionNotEnoughAuxiliary(t *testing.T) {
	env := newListenerTestEnv(t)

	validatorSet := metadata.NodeBLSMappings{{NodeID: testNodeID, Weight: 1}, {NodeID: avalanchego.NodeID{2}, Weight: 1}}
	auxApp := &stubAuxInfoApp{sufficient: false, generated: []byte("more aux info")}
	block := newTransitionBlock(t, 100, validatorSet, auxApp)

	require.NoError(t, env.listener.onIndex(block))
	require.Empty(t, env.epochs)

	// the generated auxiliary info should be broadcast instead of an approval
	require.Len(t, env.broadcaster.messages, 1)
	msg := env.broadcaster.messages[0]
	require.Nil(t, msg.EpochTransitionApproval)
	require.NotNil(t, msg.AuxiliaryInfo)
	require.Equal(t, auxApp.DefaultVersionID(), msg.AuxiliaryInfo.Version)
	require.Equal(t, auxApp.generated, msg.AuxiliaryInfo.Data)
}

func TestTransitionBroadcastsApproval(t *testing.T) {
	const nextPChainRef = uint64(100)

	env := newListenerTestEnv(t)

	validatorSet := metadata.NodeBLSMappings{{NodeID: testNodeID, Weight: 1}}
	block := newTransitionBlock(t, nextPChainRef, validatorSet, &stubAuxInfoApp{sufficient: true})
	block.msm.InitializeApprovalStore(validatorSet)

	require.NoError(t, env.listener.onIndex(block))
	require.Empty(t, env.epochs)

	require.Len(t, env.broadcaster.messages, 1)
	msg := env.broadcaster.messages[0]
	require.Nil(t, msg.AuxiliaryInfo)
	require.NotNil(t, msg.EpochTransitionApproval)

	approval := msg.EpochTransitionApproval.Approval
	require.Equal(t, testNodeID, approval.NodeID)
	require.Equal(t, nextPChainRef, approval.PChainHeight)
	require.Equal(t, [32]byte{}, approval.AuxInfoDigest) // no auxiliary info was collected
	require.Equal(t, []byte("signature"), approval.Signature)

	// the approval should also have been handed to our own approval store
	storedApprovals := block.msm.Approvals()
	require.Len(t, storedApprovals, 1)
	require.Equal(t, approval, storedApprovals[0])
}
