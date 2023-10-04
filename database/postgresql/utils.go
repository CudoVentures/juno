package postgresql

import (
	"fmt"
	"io/ioutil"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"cosmossdk.io/simapp/params"

	"github.com/forbole/juno/v5/database"
	databaseconfig "github.com/forbole/juno/v5/database/config"
	"github.com/forbole/juno/v5/logging"
)

func SetupTestDb(schemaName string) (*Database, error) {
	// Create the codec
	codec := params.MakeTestEncodingConfig()

	// Build the database
	dbCfg := databaseconfig.NewDatabaseConfig(
		fmt.Sprintf("postgres://bdjuno:password@localhost:6433/bdjuno?sslmode=disable&search_path=%s", schemaName),
		"false",
		"",
		"",
		"",
		1,
		1,
		100000,
		1000,
	)
	db, err := Builder(database.NewContext(dbCfg, &codec, logging.DefaultLogger()))
	if err != nil {
		return nil, err
	}

	bigDipperDb, ok := (db).(*Database)
	if !ok {
		return nil, fmt.Errorf("cannot convert to postgres.Database")
	}
	// Delete the public schema
	_, err = bigDipperDb.SQL.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE;`, schemaName))
	if err != nil {
		return nil, err
	}

	// Re-create the schema
	_, err = bigDipperDb.SQL.Exec(fmt.Sprintf(`CREATE SCHEMA %s;`, schemaName))
	if err != nil {
		return nil, err
	}

	dirPath := path.Join("..", "database", "postgresql")
	dir, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	for _, fileInfo := range dir {
		if !strings.Contains(fileInfo.Name(), ".sql") {
			continue
		}

		file, err := ioutil.ReadFile(filepath.Join(dirPath, fileInfo.Name()))
		if err != nil {
			return nil, err
		}

		commentsRegExp := regexp.MustCompile(`/\*.*\*/`)
		requests := strings.Split(string(file), ";")
		for _, request := range requests {
			_, err := bigDipperDb.SQL.Exec(commentsRegExp.ReplaceAllString(request, ""))
			if err != nil {
				return nil, err
			}
		}
	}

	return bigDipperDb, nil
}
