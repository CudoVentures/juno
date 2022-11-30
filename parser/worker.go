package parser

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/forbole/juno/v2/logging"

	"github.com/cosmos/cosmos-sdk/codec"

	"github.com/forbole/juno/v2/database"
	"github.com/forbole/juno/v2/types/config"

	"github.com/forbole/juno/v2/modules"

	sdk "github.com/cosmos/cosmos-sdk/types"
	tmctypes "github.com/tendermint/tendermint/rpc/core/types"
	tmtypes "github.com/tendermint/tendermint/types"

	"github.com/forbole/juno/v2/node"
	"github.com/forbole/juno/v2/types"
	"github.com/forbole/juno/v2/types/utils"
)

// Worker defines a job consumer that is responsible for getting and
// aggregating block and associated data and exporting it to a database.
type Worker struct {
	index int

	queue   types.HeightQueue
	codec   codec.Codec
	modules []modules.Module

	node   node.Node
	db     database.Database
	logger logging.Logger
}

// NewWorker allows to create a new Worker implementation.
func NewWorker(index int, ctx *Context) Worker {
	return Worker{
		index:   index,
		codec:   ctx.Codec,
		node:    ctx.Node,
		queue:   ctx.Queue,
		db:      ctx.Database,
		modules: ctx.Modules,
		logger:  ctx.Logger,
	}
}

// Start starts a worker by listening for new jobs (block heights) from the
// given worker queue. Any failed job is logged and re-enqueued.
func (w Worker) Start() {
	logging.WorkerCount.Inc()

	for i := range w.queue {
		if err := w.ProcessIfNotExists(i); err != nil {
			// re-enqueue any failed job
			// TODO: Implement exponential backoff or max retries for a block height.
			go func() {
				w.logger.Error("re-enqueueing failed block", "height", i, "err", err)
				w.queue <- i
			}()
		}

		logging.WorkerHeight.WithLabelValues(fmt.Sprintf("%d", w.index)).Set(float64(i))
	}
}

// ProcessIfNotExists defines the job consumer workflow. It will fetch a block for a given
// height and associated metadata and export it to a database if it does not exist yet. It returns an
// error if any export process fails.
func (w Worker) ProcessIfNotExists(height int64) error {
	exists, err := w.db.HasBlock(height)
	if err != nil {
		return fmt.Errorf("error while searching for block: %s", err)
	}

	if !exists {
		return w.Process(height)
	}

	parsedDataRow, err := w.db.GetBlockParsedData(height)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	p := NewBlockParsedDataFromDbRow(parsedDataRow)

	if p.HasMissingData() {
		err = w.ProcessMissingData(height, p)
		if err := w.db.SaveBlockParsedData(p.toDbRow()); err != nil {
			return err
		}
		return err
	}
	w.logger.Debug("skipping already exported block", "height", height)
	return nil
}

func (w Worker) ProcessMissingData(height int64, p *blockParsedData) error {
	b, err := node.FetchBlock(height, w.node)
	if err != nil {
		return err
	}

	if !p.vals {
		if err := w.SaveValidators(b.Vals.Validators); err != nil {
			return err
		}
		p.vals = true
	}

	if !p.block {
		if err := w.db.SaveBlock(types.NewBlockFromTmBlock(b.Block, sumGasTxs(b.Txs))); err != nil {
			return err
		}
		p.block = true
	}

	if !p.commits {
		if err := w.ExportCommit(b.Block.Block.LastCommit, b.Vals); err != nil {
			return err
		}
		p.commits = true
	}

	if !p.allBlockModules {
		w.processBlockModules(b.Block, b.Events, b.Txs, b.Vals, p)
	}

	txs := []*types.Tx{}
	for _, tx := range b.Txs {
		if !p.IsTxParsed(tx.TxHash) {
			txs = append(txs, tx)
		}
	}

	return w.ExportTxs(txs, p)
}

// Process fetches  a block for a given height and associated metadata and export it to a database.
// It returns an error if any export process fails.
func (w Worker) Process(height int64) error {
	if height == 0 {
		cfg := config.Cfg.Parser

		genesisDoc, genesisState, err := utils.GetGenesisDocAndState(cfg.GenesisFilePath, w.node)
		if err != nil {
			return fmt.Errorf("failed to get genesis: %s", err)
		}

		return w.HandleGenesis(genesisDoc, genesisState)
	}

	w.logger.Debug("processing block", "height", height)

	b, err := node.FetchBlock(height, w.node)
	if err != nil {
		return err
	}

	p, err := w.ExportBlock(b.Block, b.Events, b.Txs, b.Vals)

	if err := w.db.SaveBlockParsedData(p.toDbRow()); err != nil {
		return err
	}

	return err
}

// HandleGenesis accepts a GenesisDoc and calls all the registered genesis handlers
// in the order in which they have been registered.
func (w Worker) HandleGenesis(genesisDoc *tmtypes.GenesisDoc, appState map[string]json.RawMessage) error {
	// Call the genesis handlers
	for _, module := range w.modules {
		if genesisModule, ok := module.(modules.GenesisModule); ok {
			if err := genesisModule.HandleGenesis(genesisDoc, appState); err != nil {
				w.logger.GenesisError(module, err)
			}
		}
	}

	return nil
}

// SaveValidators persists a list of Tendermint validators with an address and a
// consensus public key. An error is returned if the public key cannot be Bech32
// encoded or if the DB write fails.
func (w Worker) SaveValidators(vals []*tmtypes.Validator) error {
	var validators = make([]*types.Validator, len(vals))
	for index, val := range vals {
		consAddr := sdk.ConsAddress(val.Address).String()

		consPubKey, err := types.ConvertValidatorPubKeyToBech32String(val.PubKey)
		if err != nil {
			return fmt.Errorf("failed to convert validator public key for validators %s: %s", consAddr, err)
		}

		validators[index] = types.NewValidator(consAddr, consPubKey)
	}

	err := w.db.SaveValidators(validators)
	if err != nil {
		return fmt.Errorf("error while saving validators: %s", err)
	}

	return nil
}

// ExportBlock accepts a finalized block and a corresponding set of transactions
// and persists them to the database along with attributable metadata. An error
// is returned if the write fails.
func (w Worker) ExportBlock(
	b *tmctypes.ResultBlock, r *tmctypes.ResultBlockResults, txs []*types.Tx, vals *tmctypes.ResultValidators,
) (*blockParsedData, error) {
	// todo here for some reason gets assigned 0 during test
	p := blockParsedData{height: b.Block.Height}
	// Save all validators
	err := w.SaveValidators(vals.Validators)
	if err != nil {
		return &p, err
	}
	p.vals = true

	// Make sure the proposer exists
	proposerAddr := sdk.ConsAddress(b.Block.ProposerAddress)
	val := findValidatorByAddr(proposerAddr.String(), vals)
	if val == nil {
		return &p, fmt.Errorf("failed to find validator by proposer address %s: %s", proposerAddr.String(), err)
	}

	// Save the block
	err = w.db.SaveBlock(types.NewBlockFromTmBlock(b, sumGasTxs(txs)))
	if err != nil {
		return &p, fmt.Errorf("failed to persist block: %s", err)
	}
	p.block = true

	// Save the commits
	err = w.ExportCommit(b.Block.LastCommit, vals)
	if err != nil {
		return &p, err
	}
	p.commits = true

	w.processBlockModules(b, r, txs, vals, &p)

	// Export the transactions
	err = w.ExportTxs(txs, &p)
	return &p, err
}

// Calls the block handlers
func (w Worker) processBlockModules(
	b *tmctypes.ResultBlock, r *tmctypes.ResultBlockResults, t []*types.Tx, v *tmctypes.ResultValidators, p *blockParsedData,
) {
	p.allBlockModules = true
	for _, module := range w.modules {
		if blockModule, ok := module.(modules.BlockModule); ok && !p.IsBlockModuleParsed(module.Name()) {
			err := blockModule.HandleBlock(b, r, t, v)
			if err != nil {
				w.logger.BlockError(module, b, err)
				p.allBlockModules = false
			} else {
				p.blockModules = append(p.blockModules, module.Name())
			}
		}
	}
}

// ExportCommit accepts a block commitment and a corresponding set of
// validators for the commitment and persists them to the database. An error is
// returned if any write fails or if there is any missing aggregated data.
func (w Worker) ExportCommit(commit *tmtypes.Commit, vals *tmctypes.ResultValidators) error {
	var signatures []*types.CommitSig
	for _, commitSig := range commit.Signatures {
		// Avoid empty commits
		if commitSig.Signature == nil {
			continue
		}

		valAddr := sdk.ConsAddress(commitSig.ValidatorAddress)
		val := findValidatorByAddr(valAddr.String(), vals)
		if val == nil {
			return fmt.Errorf("failed to find validator by commit validator address %s", valAddr.String())
		}

		signatures = append(signatures, types.NewCommitSig(
			types.ConvertValidatorAddressToBech32String(commitSig.ValidatorAddress),
			val.VotingPower,
			val.ProposerPriority,
			commit.Height,
			commitSig.Timestamp,
		))
	}

	err := w.db.SaveCommitSignatures(signatures)
	if err != nil {
		return fmt.Errorf("error while saving commit signatures: %s", err)
	}

	return nil
}

// ExportTxs accepts a slice of transactions and persists then inside the database.
// An error is returned if the write fails.
func (w Worker) ExportTxs(txs []*types.Tx, p *blockParsedData) error {
	p.allTxs = true
	// Handle all the transactions inside the block
mainloop:
	for _, tx := range txs {
		// Save the transaction itself
		err := w.db.SaveTx(tx)
		if err != nil {
			w.logger.Error(fmt.Sprintf("failed to handle transaction with hash %s: %s", tx.TxHash, err))
			p.allTxs = false
			continue
		}

		// Call the tx handlers
		for _, module := range w.modules {
			if transactionModule, ok := module.(modules.TransactionModule); ok {
				err = transactionModule.HandleTx(tx)
				if err != nil {
					w.logger.TxError(module, tx, err)
					p.allTxs = false
					continue mainloop
				}
			}
		}

		// Handle all the messages contained inside the transaction
		for i, msg := range tx.Body.Messages {
			var stdMsg sdk.Msg
			err = w.codec.UnpackAny(msg, &stdMsg)
			if err != nil {
				w.logger.Error(fmt.Sprintf("error while unpacking message: %s", err))
				p.allTxs = false
				continue mainloop
			}

			// Call the handlers
			for _, module := range w.modules {
				if messageModule, ok := module.(modules.MessageModule); ok {
					err = messageModule.HandleMsg(i, stdMsg, tx)
					if err != nil {
						w.logger.MsgError(module, tx, stdMsg, err)
						p.allTxs = false
						continue mainloop
					}
				}
			}
		}
		p.txs = append(p.txs, tx.TxHash)
	}
	return nil
}
