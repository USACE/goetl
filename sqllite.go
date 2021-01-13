package dbetl

import (
	"fmt"
	"reflect"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

const sqlitedrivername = "sqlite3"

type SqliteConfig struct {
	Dbpath string
}

type SqliteDb struct {
	db *sqlx.DB
}

func NewSqliteDb(c SqliteConfig) (*SqliteDb, error) {
	db, err := sqlx.Open(sqlitedrivername, c.Dbpath)
	if err != nil {
		return nil, err
	}
	return &SqliteDb{db}, nil
}

func (sqdb *SqliteDb) GetRows(table *Table) (*sqlx.Rows, error) {
	return sqdb.db.Queryx(table.SelectSql)
}

func (sqdb *SqliteDb) StartTransaction() (*sqlx.Tx, error) {
	return sqdb.db.Beginx()
}

func (sqldb *SqliteDb) TableExists(name string) (bool, error) {
	sql := fmt.Sprintf(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name='%s'`, name)
	var exists int
	err := sqldb.db.Get(&exists, sql)
	return exists > 0, err
}

func (sqldb *SqliteDb) CreateTable(table *Table) error {
	sql := createTableSql(table.Name, reflect.TypeOf(table.Fields))
	_, err := sqldb.db.Exec(sql)
	return err
}

func (sqldb *SqliteDb) Close() {
	sqldb.db.Close()
}
