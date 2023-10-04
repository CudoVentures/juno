package database

// We save Txs and BlockModules in the db as a single string joined by ","
type BlockParsedDataRow struct {
	Height          int64  `db:"height"`
	Txs             string `db:"txs"`
	BlockModules    string `db:"block_modules"`
	Validators      bool   `db:"validators"`
	Block           bool   `db:"block"`
	Commits         bool   `db:"commits"`
	AllTxs          bool   `db:"all_txs"`
	AllBlockModules bool   `db:"all_block_modules"`
}
