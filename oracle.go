package dbetl

import (
	"fmt"

	_ "github.com/godror/godror"
	"github.com/jmoiron/sqlx"
)

const oracledrivername = "godror"

type OracleConfig struct {
	Dbuser        string
	Dbpass        string
	Dbname        string
	Dbhost        string
	InstantClient string
}

type OracleDb struct {
	db *sqlx.DB
}

func NewOracleDb(c OracleConfig) (*OracleDb, error) {
	db, err := sqlx.Open(oracledrivername, fmt.Sprintf(`user="%s" password="%s" connectString="%s:1521/%s" libDir="%s"`,
		c.Dbuser, c.Dbpass, c.Dbhost, c.Dbname, c.InstantClient))
	if err != nil {
		return nil, err
	}
	return &OracleDb{db}, nil
}

func (odb *OracleDb) GetRows(table *Table) (*sqlx.Rows, error) {
	return odb.db.Queryx(table.SelectSql)
}

func (odb *OracleDb) StartTransaction() (*sqlx.Tx, error) {
	return odb.db.Beginx()
}
