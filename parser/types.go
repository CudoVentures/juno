package parser

import (
	"strings"

	"github.com/forbole/juno/v2/database"
)

// blockParsedData keeps track of parsed contents during block processing
// it is then used to refetch missing data
type blockParsedData struct {
	height          int64
	vals            bool
	block           bool
	commits         bool
	txs             []string
	allTxs          bool
	blockModules    []string
	allBlockModules bool
}

func NewBlockParsedDataFromDbRow(t *database.BlockParsedDataRow) *blockParsedData {
	txsUnparsed := []string{}
	if t.Txs != "" {
		txsUnparsed = strings.Split(t.Txs, ",")
	}
	blockModulesUnparsed := []string{}
	if t.BlockModules != "" {
		blockModulesUnparsed = strings.Split(t.BlockModules, ",")
	}

	return &blockParsedData{
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

func (b *blockParsedData) HasMissingData() bool {
	return !(b.vals && b.block && b.commits && b.allBlockModules && b.allTxs)
}

func (b *blockParsedData) IsBlockModuleParsed(moduleName string) bool {
	for _, m := range b.blockModules {
		if m == moduleName {
			return true
		}
	}
	return false
}

func (b *blockParsedData) IsTxParsed(txHash string) bool {
	for _, t := range b.txs {
		if t == txHash {
			return true
		}
	}
	return false
}

func (b *blockParsedData) toDbRow() *database.BlockParsedDataRow {
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
