package node

import (
	"fmt"

	"github.com/forbole/juno/v2/types"
	tmctypes "github.com/tendermint/tendermint/rpc/core/types"
)

type Block struct {
	Block  *tmctypes.ResultBlock
	Events *tmctypes.ResultBlockResults
	Txs    []*types.Tx
	Vals   *tmctypes.ResultValidators
}

func FetchBlock(height int64, node Node) (*Block, error) {
	block, err := node.Block(height)
	if err != nil {
		return nil, fmt.Errorf("failed to get block from node: %s", err)
	}

	events, err := node.BlockResults(height)
	if err != nil {
		return nil, fmt.Errorf("failed to get block results from node: %s", err)
	}

	txs, err := node.Txs(block)
	if err != nil {
		return nil, fmt.Errorf("failed to get transactions for block: %s", err)
	}

	vals, err := node.Validators(height)
	if err != nil {
		return nil, fmt.Errorf("failed to get validators for block: %s", err)
	}

	return &Block{block, events, txs, vals}, nil
}
