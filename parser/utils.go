package parser

import (
	"strings"

	tmctypes "github.com/cometbft/cometbft/rpc/core/types"
	tmtypes "github.com/cometbft/cometbft/types"
	sdk "github.com/cosmos/cosmos-sdk/types"

	"github.com/forbole/juno/v5/database"
	"github.com/forbole/juno/v5/types"
)

// findValidatorByAddr finds a validator by a consensus address given a set of
// Tendermint validators for a particular block. If no validator is found, nil
// is returned.
func findValidatorByAddr(consAddr string, vals *tmctypes.ResultValidators) *tmtypes.Validator {
	for _, val := range vals.Validators {
		if consAddr == sdk.ConsAddress(val.Address).String() {
			return val
		}
	}

	return nil
}

// sumGasTxs returns the total gas consumed by a set of transactions.
func sumGasTxs(txs []*types.Tx) uint64 {
	var totalGas uint64

	for _, tx := range txs {
		totalGas += uint64(tx.GasUsed)
	}

	return totalGas
}

// blockParsedData keeps track of parsed contents during block processing
// it is then used to refetch missing data
type BlockParsedData struct {
	height          int64
	txs             []string
	blockModules    []string
	vals            bool
	block           bool
	commits         bool
	allTxs          bool
	allBlockModules bool
}

func NewBlockParsedDataFromDbRow(t *database.BlockParsedDataRow) *BlockParsedData {
	txsUnparsed := []string{}
	if t.Txs != "" {
		txsUnparsed = strings.Split(t.Txs, ",")
	}
	blockModulesUnparsed := []string{}
	if t.BlockModules != "" {
		blockModulesUnparsed = strings.Split(t.BlockModules, ",")
	}

	return &BlockParsedData{
		height:          t.Height,
		vals:            t.Validators,
		block:           t.Block,
		commits:         t.Commits,
		txs:             txsUnparsed,
		allTxs:          t.AllTxs,
		blockModules:    blockModulesUnparsed,
		allBlockModules: t.AllBlockModules,
	}
}

func (b *BlockParsedData) HasMissingData() bool {
	return !(b.vals && b.block && b.commits && b.allBlockModules && b.allTxs)
}

func (b *BlockParsedData) IsBlockModuleParsed(moduleName string) bool {
	for _, m := range b.blockModules {
		if m == moduleName {
			return true
		}
	}
	return false
}

func (b *BlockParsedData) IsTxParsed(txHash string) bool {
	for _, t := range b.txs {
		if t == txHash {
			return true
		}
	}
	return false
}

func (b *BlockParsedData) toDbRow() *database.BlockParsedDataRow {
	return &database.BlockParsedDataRow{
		Height:          b.height,
		Validators:      b.vals,
		Block:           b.block,
		Commits:         b.commits,
		Txs:             strings.Join(b.txs, ","),
		AllTxs:          b.allTxs,
		BlockModules:    strings.Join(b.blockModules, ","),
		AllBlockModules: b.allBlockModules,
	}
}
