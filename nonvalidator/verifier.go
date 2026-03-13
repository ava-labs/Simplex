package nonvalidator

import (
	"context"

	"github.com/ava-labs/simplex"
	"go.uber.org/zap"
)

// Verifier verifies blocks in order, one at a time.
type Verifiier struct {
	ctx       context.Context
	cancelCtx context.CancelFunc

	logger              simplex.Logger
	latestVerifiedBlock simplex.Block     // last block we have verified
	storage             simplex.Retriever // access to blocks
	scheduler           *simplex.BasicScheduler
}

func NewVerifier(logger simplex.Logger, lastVerifiedBlock simplex.Block, storgae simplex.Retriever) *Verifiier {
	return &Verifiier{
		logger:              logger,
		latestVerifiedBlock: lastVerifiedBlock,
		storage:             storgae,
		scheduler:           simplex.NewScheduler(logger, 1),
	}
}

func (v *Verifiier) nextSeqToVerify() uint64 {
	if v.latestVerifiedBlock == nil {
		return 0
	}

	return v.latestVerifiedBlock.BlockHeader().Seq + 1
}

// alreadyVerifiedSeq checks if `seq` has already been verified.
func (v *Verifiier) alreadyVerifiedSeq(seq uint64) bool {
	return seq < v.nextSeqToVerify()
}

// triggerVerify wakes up the verifier by attempting to verify the next seq to be verified.
// We verify `block` if it is the next sequence to be verified, otherwise we try to retrieve the next
// block to be verified from storage.
func (v *Verifiier) triggerVerify(block simplex.Block) error {
	nextSeqToVerify := v.nextSeqToVerify()
	if block.BlockHeader().Seq != nextSeqToVerify {
		block, _, err := v.storage.Retrieve(nextSeqToVerify)
		if err == simplex.ErrBlockNotFound {
			return nil
		} else if err != nil {
			return err
		}

		// Re-call trigger verify and schedule the block verification task
		return v.triggerVerify(block)
	}

	task := v.createBlockVerificationTask(block)
	v.scheduler.Schedule(task)
	return nil
}

func (v *Verifiier) createBlockVerificationTask(block simplex.Block) func() simplex.Digest {
	return func() simplex.Digest {
		_, err := block.Verify(v.ctx)
		if err != nil {
			v.logger.Error("Error verifying block with a finalization", zap.Uint64("Seq", block.BlockHeader().Seq), zap.Error(err))
			return simplex.Digest{}
		}

		v.latestVerifiedBlock = block

		if err := v.triggerVerify(block); err != nil {
			v.logger.Warn("Error while calling triggerVerify", zap.Error(err))
			// TODO: stop & halt the verifier + notify parent components
		}

		return simplex.Digest{}
	}
}
