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
	// approvals records the approvals fed back to the local store via the handleApproval
	// callback. It stays empty for a non-validator listener (nil handleApproval).
	approvals []common.ValidatorSetApproval
	listener  *epochTransitionListener
}

// newListenerTestEnv builds a listener wired to the given next-epoch validator set and
// auxiliary info app. When isValidator is true, the listener is given a handleApproval
// callback (recording into env.approvals) as a real validator MSM would; otherwise it is
// nil, matching a non-validator that has no block builder to record its own approval.
func newListenerTestEnv(t *testing.T, validatorSet metadata.NodeBLSMappings, auxApp metadata.AuxiliaryInfoGenVerifier, isValidator bool) *listenerTestEnv {
	env := &listenerTestEnv{broadcaster: &recordingBroadcaster{}}

	getValidatorSet := func(uint64) (metadata.NodeBLSMappings, error) {
		return validatorSet, nil
	}
	getBlock := func(seq uint64, _ common.Digest) (metadata.StateMachineBlock, *common.Finalization, error) {
		require.Fail(t, "unexpected getBlock call", "seq %d", seq)
		return metadata.StateMachineBlock{}, nil, nil
	}

	var handleApproval func(approval *common.ValidatorSetApproval, timestamp uint64) error
	if isValidator {
		handleApproval = func(approval *common.ValidatorSetApproval, _ uint64) error {
			env.approvals = append(env.approvals, *approval)
			return nil
		}
	}

	env.listener = newEpochTransitionListener(
		testutil.MakeLogger(t, 1),
		env.broadcaster,
		testNodeID,
		getValidatorSet,
		getBlock,
		stubSigner{sig: []byte("signature")},
		auxApp,
		handleApproval,
		func(epoch uint64, validators common.Nodes) error {
			env.epochs = append(env.epochs, epochChange{
				epoch:      epoch,
				validators: validators,
			})
			return nil
		},
	)
	return env
}

// newTransitionBlock returns a ParsedBlock of type BlockTypeTransitioning carrying the
// given next-epoch P-chain reference height. The listener supplies the validator set and
// auxiliary info app, so the block itself needs no MSM.
func newTransitionBlock(t *testing.T, nextPChainRef uint64) *ParsedBlock {
	block := &ParsedBlock{
		StateMachineBlock: metadata.StateMachineBlock{
			Metadata: metadata.StateMachineMetadata{
				SimplexProtocolMetadata: (&common.ProtocolMetadata{Seq: 10}).Bytes(),
				SimplexEpochInfo: metadata.SimplexEpochInfo{
					NextPChainReferenceHeight: nextPChainRef,
				},
			},
		},
	}
	require.Equal(t, metadata.BlockTypeTransitioning, block.Type())
	return block
}

func TestSealingBlockCallback(t *testing.T) {
	const sealingSeq = uint64(42)

	env := newListenerTestEnv(t, nil, nil, true)

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
	// the next validator set does not contain our node
	otherValidator := metadata.NodeBLSMappings{{NodeID: avalanchego.NodeID{2}, Weight: 1}}
	auxApp := &stubAuxInfoApp{sufficient: false, generated: []byte("more aux info")}
	env := newListenerTestEnv(t, otherValidator, auxApp, true)

	block := newTransitionBlock(t, 100)

	require.NoError(t, env.listener.onIndex(block))
	require.Empty(t, env.broadcaster.messages)
	require.Empty(t, env.epochs)
	require.Empty(t, env.approvals)
}

func TestTransitionNotEnoughAuxiliary(t *testing.T) {
	validatorSet := metadata.NodeBLSMappings{{NodeID: testNodeID, Weight: 1}, {NodeID: avalanchego.NodeID{2}, Weight: 1}}
	auxApp := &stubAuxInfoApp{sufficient: false, generated: []byte("more aux info")}
	env := newListenerTestEnv(t, validatorSet, auxApp, true)

	block := newTransitionBlock(t, 100)

	require.NoError(t, env.listener.onIndex(block))
	require.Empty(t, env.epochs)

	// the generated auxiliary info should be broadcast instead of an approval
	require.Len(t, env.broadcaster.messages, 1)
	msg := env.broadcaster.messages[0]
	require.Nil(t, msg.EpochTransitionApproval)
	require.NotNil(t, msg.AuxiliaryInfo)
	require.Equal(t, auxApp.DefaultVersionID(), msg.AuxiliaryInfo.Version)
	require.Equal(t, auxApp.generated, msg.AuxiliaryInfo.Data)
	require.Empty(t, env.approvals)
}

func TestTransitionBroadcastsApproval(t *testing.T) {
	const nextPChainRef = uint64(100)

	validatorSet := metadata.NodeBLSMappings{{NodeID: testNodeID, Weight: 1}}
	env := newListenerTestEnv(t, validatorSet, &stubAuxInfoApp{sufficient: true}, true)

	block := newTransitionBlock(t, nextPChainRef)

	require.NoError(t, env.listener.onIndex(block))
	require.Empty(t, env.epochs)

	require.Len(t, env.broadcaster.messages, 1)
	msg := env.broadcaster.messages[0]
	require.Nil(t, msg.AuxiliaryInfo)
	require.NotNil(t, msg.EpochTransitionApproval)

	approval := msg.EpochTransitionApproval
	require.Equal(t, testNodeID, approval.NodeID)
	require.Equal(t, nextPChainRef, approval.PChainHeight)
	require.Equal(t, [32]byte{}, approval.AuxInfoDigest) // no auxiliary info was collected
	require.Equal(t, []byte("signature"), approval.Signature)

	// a validator also records its own approval locally so its next block includes it
	require.Equal(t, []common.ValidatorSetApproval{*approval}, env.approvals)
}

// TestNonValidatorContributesAuxiliaryInfo asserts that a node still on the outside of the
// current validator set but present in the NEXT one contributes auxiliary info during the
// transition, exactly like a validator does. Non-validators pass a nil handleApproval, so
// nothing is recorded locally, but the auxiliary info is still broadcast.
func TestNonValidatorContributesAuxiliaryInfo(t *testing.T) {
	validatorSet := metadata.NodeBLSMappings{{NodeID: testNodeID, Weight: 1}, {NodeID: avalanchego.NodeID{2}, Weight: 1}}
	auxApp := &stubAuxInfoApp{sufficient: false, generated: []byte("non-validator aux")}
	env := newListenerTestEnv(t, validatorSet, auxApp, false /* non-validator */)

	block := newTransitionBlock(t, 100)

	require.NoError(t, env.listener.onIndex(block))

	require.Len(t, env.broadcaster.messages, 1)
	msg := env.broadcaster.messages[0]
	require.Nil(t, msg.EpochTransitionApproval)
	require.NotNil(t, msg.AuxiliaryInfo)
	require.Equal(t, auxApp.DefaultVersionID(), msg.AuxiliaryInfo.Version)
	require.Equal(t, auxApp.generated, msg.AuxiliaryInfo.Data)

	// non-validators do not record approvals locally
	require.Empty(t, env.approvals)
	require.Empty(t, env.epochs)
}

// TestNonValidatorContributesApproval asserts that once the auxiliary info history is
// sufficient, a non-validator that belongs to the next validator set broadcasts its
// epoch transition approval. It does not record the approval locally (nil handleApproval),
// since it has no block builder to include it.
func TestNonValidatorContributesApproval(t *testing.T) {
	const nextPChainRef = uint64(100)

	validatorSet := metadata.NodeBLSMappings{{NodeID: testNodeID, Weight: 1}}
	env := newListenerTestEnv(t, validatorSet, &stubAuxInfoApp{sufficient: true}, false /* non-validator */)

	block := newTransitionBlock(t, nextPChainRef)

	require.NoError(t, env.listener.onIndex(block))

	require.Len(t, env.broadcaster.messages, 1)
	msg := env.broadcaster.messages[0]
	require.Nil(t, msg.AuxiliaryInfo)
	require.NotNil(t, msg.EpochTransitionApproval)

	approval := msg.EpochTransitionApproval
	require.Equal(t, testNodeID, approval.NodeID)
	require.Equal(t, nextPChainRef, approval.PChainHeight)
	require.Equal(t, []byte("signature"), approval.Signature)

	// non-validators broadcast but do not record their own approval locally
	require.Empty(t, env.approvals)
	require.Empty(t, env.epochs)
}
