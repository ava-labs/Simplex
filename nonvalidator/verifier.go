package nonvalidator

import (
	"context"
	"errors"
	"sync"

	"github.com/ava-labs/simplex"
	"go.uber.org/zap"
)

// Verifier verifies blocks in order, one at a time.
type Verifier struct {
	ctx context.Context

	lock                sync.Mutex
	logger              simplex.Logger
	latestVerifiedBlock simplex.Block     // last block we have verified
	storage             simplex.Retriever // access to blocks
	scheduler           *simplex.BasicScheduler
	oneTimeVerifier     *simplex.OneTimeVerifier
}

func NewVerifier(ctx context.Context, logger simplex.Logger, lastVerifiedBlock simplex.Block, storage simplex.Retriever) *Verifier {
	return &Verifier{
		ctx:                 ctx,
		logger:              logger,
		latestVerifiedBlock: lastVerifiedBlock,
		storage:             storage,
		scheduler:           simplex.NewScheduler(logger, 1),
		oneTimeVerifier:     simplex.NewOneTimeVerifier(logger),
	}
}

// latestVerified returns the most recently verified block.
func (v *Verifier) latestVerified() simplex.Block {
	v.lock.Lock()
	defer v.lock.Unlock()
	return v.latestVerifiedBlock
}

func (v *Verifier) nextSeqToVerify() uint64 {
	if v.latestVerifiedBlock == nil {
		return 0
	}
	return v.latestVerifiedBlock.BlockHeader().Seq + 1
}

// alreadyVerifiedSeq checks if `seq` has already been verified.
func (v *Verifier) alreadyVerifiedSeq(seq uint64) bool {
	return seq < v.nextSeqToVerify()
}

// triggerVerify wakes up the verifier by attempting to verify the next seq to be verified.
// We verify `block` if it is the next sequence to be verified, otherwise we try to retrieve the next
// block to be verified from storage.
func (v *Verifier) triggerVerify(block simplex.Block) error {
	v.lock.Lock()
	defer v.lock.Unlock()

	nextSeq := v.nextSeqToVerify()
	for {
		if block.BlockHeader().Seq == nextSeq {
			v.scheduler.Schedule(v.createBlockVerificationTask(v.oneTimeVerifier.Wrap(block)))
			return nil
		}
		retrieved, _, err := v.storage.Retrieve(nextSeq)
		if errors.Is(err, simplex.ErrBlockNotFound) {
			return nil
		} else if err != nil {
			return err
		}
		block = retrieved
	}
}

func (v *Verifier) createBlockVerificationTask(block simplex.Block) func() simplex.Digest {
	return func() simplex.Digest {
		_, err := block.Verify(v.ctx)
		if err != nil {
			v.logger.Error("Error verifying block with a finalization", zap.Uint64("Seq", block.BlockHeader().Seq), zap.Error(err))
			return simplex.Digest{}
		}

		v.lock.Lock()
		v.latestVerifiedBlock = block
		v.lock.Unlock()

		if err := v.triggerVerify(block); err != nil {
			v.logger.Warn("Error while calling triggerVerify", zap.Error(err))
			// TODO: stop & halt the verifier + notify parent components
		}

		return simplex.Digest{}
	}
}
