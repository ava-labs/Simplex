package random_network

import (
	"github.com/ava-labs/simplex"
	"github.com/ava-labs/simplex/testutil"
)

type Node struct {
	storage *testutil.InMemStorage
	mempool *Mempool

	epoch *simplex.Epoch
}

func NewNode() *Node {
	return &Node{

	}
}