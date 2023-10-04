package parser

import (
	"fmt"
	"testing"

	params "cosmossdk.io/simapp/params"
	"github.com/cometbft/cometbft/libs/bytes"
	tmctypes "github.com/cometbft/cometbft/rpc/core/types"
	tmtypes "github.com/cometbft/cometbft/types"
	cosmostypes "github.com/cosmos/cosmos-sdk/codec/types"
	sdk "github.com/cosmos/cosmos-sdk/types"
	"github.com/cosmos/cosmos-sdk/types/tx"
	"github.com/forbole/juno/v5/database"
	"github.com/forbole/juno/v5/database/postgresql"
	"github.com/forbole/juno/v5/logging"
	"github.com/forbole/juno/v5/modules"
	"github.com/forbole/juno/v5/node/remote"
	"github.com/forbole/juno/v5/types"
	"github.com/stretchr/testify/require"
)

func TestParseWorker(t *testing.T) {
	for testName, tc := range map[string]struct {
		arrange func(db *mockDb, n *mockNode, m *mockModule, p *database.BlockParsedDataRow)
		wantErr error
	}{
		"happy path": {
			arrange: func(db *mockDb, n *mockNode, m *mockModule, p *database.BlockParsedDataRow) {},
		},
		"err HandleMsg": {
			arrange: func(db *mockDb, n *mockNode, m *mockModule, p *database.BlockParsedDataRow) {
				m.handleMsgErr = fmt.Errorf("")
				p.Txs = ""
				p.AllTxs = false
			},
		},
		"err UnpackAny": {
			arrange: func(db *mockDb, n *mockNode, m *mockModule, p *database.BlockParsedDataRow) {
				n.msg.TypeUrl = "invalid"
				p.Txs = ""
				p.AllTxs = false
			},
		},
		"err SaveTx": {
			arrange: func(db *mockDb, n *mockNode, m *mockModule, p *database.BlockParsedDataRow) {
				db.saveTxErr = fmt.Errorf("")
				p.Txs = ""
				p.AllTxs = false
			},
		},
		"err HandleBlock": {
			arrange: func(db *mockDb, n *mockNode, m *mockModule, p *database.BlockParsedDataRow) {
				m.handleBlockErr = fmt.Errorf("")
				p.BlockModules = ""
				p.AllBlockModules = false
			},
		},
		"err ExportCommit": {
			arrange: func(db *mockDb, node *mockNode, m *mockModule, p *database.BlockParsedDataRow) {
				db.saveCommitErr = fmt.Errorf("")
				*p = database.BlockParsedDataRow{Height: 1, Validators: true, Block: true}
			},
			wantErr: fmt.Errorf("error while saving commit signatures: "),
		},
		"err SaveBlock": {
			arrange: func(db *mockDb, node *mockNode, m *mockModule, p *database.BlockParsedDataRow) {
				db.saveBlockErr = fmt.Errorf("")
				*p = database.BlockParsedDataRow{Height: 1, Validators: true}
			},
			wantErr: fmt.Errorf("failed to persist block: "),
		},
		"err SaveValidators": {
			arrange: func(db *mockDb, node *mockNode, m *mockModule, p *database.BlockParsedDataRow) {
				db.saveValidatorsErr = fmt.Errorf("")
				*p = database.BlockParsedDataRow{Height: 1}
			},
			wantErr: fmt.Errorf("error while saving validators: "),
		},
		"err SaveBlockParsedData": {
			arrange: func(db *mockDb, node *mockNode, m *mockModule, p *database.BlockParsedDataRow) {
				db.saveParsedDataErr = fmt.Errorf("")
				*p = database.BlockParsedDataRow{}
			},
			wantErr: fmt.Errorf(""),
		},
		"err FetchBlock": {
			arrange: func(db *mockDb, node *mockNode, m *mockModule, p *database.BlockParsedDataRow) {
				node.err = fmt.Errorf("")
				*p = database.BlockParsedDataRow{}
			},
			wantErr: fmt.Errorf("failed to get block from node: "),
		},
		"err GetBlockParsedData": {
			arrange: func(db *mockDb, node *mockNode, m *mockModule, p *database.BlockParsedDataRow) {
				db.hasBlock = true
				db.getParsedDataErr = fmt.Errorf("")
				*p = database.BlockParsedDataRow{}
			},
			wantErr: fmt.Errorf(""),
		},
		"ProcessMissingData happy path": {
			arrange: func(db *mockDb, node *mockNode, m *mockModule, p *database.BlockParsedDataRow) {
				db.hasBlock = true
				require.NoError(t, db.SaveBlockParsedData(&database.BlockParsedDataRow{Height: 1, BlockModules: "t", Txs: "t"}))
			},
		},
		"ProcessMissingData err ExportCommit": {
			arrange: func(db *mockDb, node *mockNode, m *mockModule, p *database.BlockParsedDataRow) {
				db.hasBlock = true
				db.saveCommitErr = fmt.Errorf("")
				*p = database.BlockParsedDataRow{Height: 1}
				require.NoError(t, db.SaveBlockParsedData(p))
				p.Validators = true
				p.Block = true
			},
			wantErr: fmt.Errorf("error while saving commit signatures: "),
		},
		"ProcessMissingData err SaveBlock": {
			arrange: func(db *mockDb, node *mockNode, m *mockModule, p *database.BlockParsedDataRow) {
				db.hasBlock = true
				db.saveBlockErr = fmt.Errorf("")
				*p = database.BlockParsedDataRow{Height: 1}
				require.NoError(t, db.SaveBlockParsedData(p))
				p.Validators = true
			},
			wantErr: fmt.Errorf(""),
		},
		"ProcessMissingData err SaveValidators": {
			arrange: func(db *mockDb, node *mockNode, m *mockModule, p *database.BlockParsedDataRow) {
				db.hasBlock = true
				db.saveValidatorsErr = fmt.Errorf("")
				*p = database.BlockParsedDataRow{Height: 1}
				require.NoError(t, db.SaveBlockParsedData(p))
			},
			wantErr: fmt.Errorf("error while saving validators: "),
		},
		"ProcessMissingData err FetchBlock": {
			arrange: func(db *mockDb, node *mockNode, m *mockModule, p *database.BlockParsedDataRow) {
				db.hasBlock = true
				node.err = fmt.Errorf("")
				*p = database.BlockParsedDataRow{Height: 1}
				require.NoError(t, db.SaveBlockParsedData(p))
			},
			wantErr: fmt.Errorf("failed to get block from node: "),
		},
		"ProcessMissingData err SaveBlockParsedData": {
			arrange: func(db *mockDb, node *mockNode, m *mockModule, p *database.BlockParsedDataRow) {
				db.hasBlock = true
				*p = database.BlockParsedDataRow{Height: 1}
				require.NoError(t, db.SaveBlockParsedData(p))
				db.saveParsedDataErr = fmt.Errorf("")
			},
			wantErr: fmt.Errorf(""),
		},
		"skip existing block with no missing data": {
			arrange: func(db *mockDb, node *mockNode, m *mockModule, p *database.BlockParsedDataRow) {
				db.hasBlock = true
				require.NoError(t, db.SaveBlockParsedData(p))
			},
		},
		"err HasBlock": {
			arrange: func(db *mockDb, node *mockNode, m *mockModule, p *database.BlockParsedDataRow) {
				db.hasBlockErr = fmt.Errorf("")
				*p = database.BlockParsedDataRow{}
			},
			wantErr: fmt.Errorf("error while searching for block: "),
		},
	} {
		t.Run(testName, func(t *testing.T) {
			queue := types.NewQueue(25)
			queue <- 1

			postgres, err := postgresql.SetupTestDb("testProcess")
			require.NoError(t, err)
			db := mockDb{db: postgres}
			node := mockNode{}
			module := mockModule{name: "t"}
			modules := []modules.Module{&module}
			p := database.BlockParsedDataRow{
				Height: 1, Validators: true, Block: true, Commits: true, Txs: "t", AllTxs: true, BlockModules: "t", AllBlockModules: true,
			}

			tc.arrange(&db, &node, &module, &p)

			codec := params.MakeTestEncodingConfig()

			ctx := NewContext(
				&codec,
				&node,
				&db,
				logging.DefaultLogger(),
				modules,
			)
			worker := NewWorker(ctx, nil, 0)
			err = worker.ProcessIfNotExists(1)
			require.Equal(t, tc.wantErr, err)

			have, _ := db.GetBlockParsedData(1)
			require.Equal(t, &p, have)
		})
	}
}

// Mock module
type mockModule struct {
	name           string
	handleBlockErr error
	handleMsgErr   error
}

func (m *mockModule) Name() string {
	return m.name
}

func (m *mockModule) HandleBlock(
	block *tmctypes.ResultBlock, _ *tmctypes.ResultBlockResults, _ []*types.Tx, _ *tmctypes.ResultValidators,
) error {
	return m.handleBlockErr
}

func (m *mockModule) HandleMsg(index int, msg sdk.Msg, tx *types.Tx) error {
	return m.handleMsgErr
}

// Mock node
type mockNode struct {
	*remote.Node
	msg cosmostypes.Any
	err error
}

func (n *mockNode) Block(height int64) (*tmctypes.ResultBlock, error) {
	block := tmtypes.MakeBlock(1, []tmtypes.Tx{}, &tmtypes.Commit{}, []tmtypes.Evidence{})
	block.ProposerAddress = bytes.HexBytes{}
	return &tmctypes.ResultBlock{
		Block: block,
	}, n.err
}

func (n *mockNode) BlockResults(height int64) (*tmctypes.ResultBlockResults, error) {
	return &tmctypes.ResultBlockResults{}, n.err
}

func (n *mockNode) Txs(block *tmctypes.ResultBlock) ([]*types.Tx, error) {
	return []*types.Tx{{
		TxResponse: &sdk.TxResponse{TxHash: "t"},
		Tx: &tx.Tx{
			Body:     &tx.TxBody{Messages: []*cosmostypes.Any{&n.msg}},
			AuthInfo: &tx.AuthInfo{Fee: &tx.Fee{Amount: sdk.NewCoins(), GasLimit: 0}},
		}},
	}, n.err
}

func (n *mockNode) Validators(height int64) (*tmctypes.ResultValidators, error) {
	return &tmctypes.ResultValidators{
		Validators: []*tmtypes.Validator{{Address: bytes.HexBytes{}, PubKey: tmtypes.NewMockPV().PrivKey.PubKey()}},
	}, n.err
}

// Mock db
type mockDb struct {
	*postgresql.Database
	db                *postgresql.Database
	saveValidatorsErr error
	saveBlockErr      error
	saveCommitErr     error
	saveTxErr         error
	saveParsedDataErr error
	getParsedDataErr  error
	hasBlockErr       error
	hasBlock          bool
}

func (db *mockDb) SaveValidators(validators []*types.Validator) error {
	return db.saveValidatorsErr
}

func (db *mockDb) SaveBlock(block *types.Block) error {
	return db.saveBlockErr
}

func (db *mockDb) SaveCommitSignatures(signatures []*types.CommitSig) error {
	return db.saveCommitErr
}

func (db *mockDb) SaveTx(tx *types.Tx) error {
	return db.saveTxErr
}

func (db *mockDb) SaveBlockParsedData(p *database.BlockParsedDataRow) error {
	if db.saveParsedDataErr != nil {
		return db.saveParsedDataErr
	}
	return db.db.SaveBlockParsedData(p)
}

func (db *mockDb) GetBlockParsedData(height int64) (*database.BlockParsedDataRow, error) {
	if db.getParsedDataErr != nil {
		return &database.BlockParsedDataRow{}, db.getParsedDataErr
	}
	return db.db.GetBlockParsedData(height)
}

func (db *mockDb) HasBlock(height int64) (bool, error) {
	return db.hasBlock, db.hasBlockErr
}
