package nonvalidator

import (
	"fmt"
	"testing"

	"github.com/ava-labs/simplex"
)

type nonValidatorResponderComm struct {
	t       *testing.T
	storage *testChain
	nodes   simplex.Nodes
}

// initializes the storgae with sealing blocks at epochs, and normal blocks inbetweem
func (r *nonValidatorResponderComm) initializeStorage(epochs ...uint64) {
	if len(epochs) < 1 || epochs[0] != 1 {
		r.t.Fatalf("Need to have the first epoch")
	}
	chain := newSeededChain(r.t, r.nodes, epochs[0])

	for _, epoch := range epochs[1:] {
		for chain.seq < epoch-1 {
			chain.appendBlock()
		}
		chain.appendSealing(r.nodes)
		fmt.Println(chain.String())
	}

	r.storage = chain
}

func TestResponder(t *testing.T) {
	r := nonValidatorResponderComm{
		nodes: testNodes,
		t:     t,
	}

	r.initializeStorage(1, 10, 20, 30)
}
