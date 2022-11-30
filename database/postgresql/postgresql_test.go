package postgresql_test

import (
	"testing"

	"github.com/stretchr/testify/suite"

	postgres "github.com/forbole/juno/v2/database/postgresql"
)

func TestDatabaseTestSuite(t *testing.T) {
	suite.Run(t, new(DbTestSuite))
}

type DbTestSuite struct {
	suite.Suite

	database *postgres.Database
}

func (suite *DbTestSuite) SetupTest() {
	db, err := postgres.SetupTestDb("public")
	suite.Require().NoError(err)
	suite.database = db
}
