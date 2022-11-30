package database

// We save Txs and BlockModules in the db as a single string joined by ","
type BlockParsedDataRow struct {
	Height          int64  `db:"height"`
	Validators      bool   `db:"validators"`
	Block           bool   `db:"block"`
	Commits         bool   `db:"commits"`
	Txs             string `db:"txs"`
	AllTxs          bool   `db:"all_txs"`
	BlockModules    string `db:"block_modules"`
	AllBlockModules bool   `db:"all_block_modules"`
}
