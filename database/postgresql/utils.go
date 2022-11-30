package postgresql

import (
	"fmt"
	"io/ioutil"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	"github.com/cosmos/cosmos-sdk/simapp"

	"github.com/forbole/juno/v2/database"
	databaseconfig "github.com/forbole/juno/v2/database/config"
	"github.com/forbole/juno/v2/logging"
)

func SetupTestDb(schemaName string) (*Database, error) {
	// Create the codec
	codec := simapp.MakeTestEncodingConfig()

	// Build the database
	dbCfg := databaseconfig.NewDatabaseConfig(
		"bdjuno",
		"localhost",
		6433,
		"bdjuno",
		"password",
		"",
		schemaName,
		-1,
		-1,
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
	_, err = bigDipperDb.Sql.Exec(fmt.Sprintf(`DROP SCHEMA IF EXISTS %s CASCADE;`, schemaName))
	if err != nil {
		return nil, err
	}

	// Re-create the schema
	_, err = bigDipperDb.Sql.Exec(fmt.Sprintf(`CREATE SCHEMA %s;`, schemaName))
	if err != nil {
		return nil, err
	}

	// Get this file's directory instead of the caller's
	_, currentDir, _, _ := runtime.Caller(0)
	dirPath := path.Join(path.Dir(currentDir))
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
			_, err := bigDipperDb.Sql.Exec(commentsRegExp.ReplaceAllString(request, ""))
			if err != nil {
				return nil, err
			}
		}
	}
	return bigDipperDb, nil
}
