package dbetl

import (
	"fmt"
	"log"
	"reflect"

	_ "github.com/godror/godror"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

type SqlDbConfig struct {
	Path        string
	Dbuser      string
	Dbpass      string
	Dbname      string
	Dbhost      string
	Dbport      int
	ExternalLib string
}

func (c *SqlDbConfig) ToDsn() string {
	return fmt.Sprintf(`user=%s password=%s host=%s port=%d dbname=%s`,
		c.Dbuser, c.Dbpass, c.Dbhost, c.Dbport, c.Dbname)
}

type SqlDbImpl struct {
	Driver         string
	Url            string
	TableExistsSql string
}

func NewOracleSqlImpl(config SqlDbConfig) (*SqlDb, error) {
	impl := SqlDbImpl{
		Driver: "godror",
		Url: fmt.Sprintf(`user="%s" password="%s" connectString="%s:%d/%s" libDir="%s"`,
			config.Dbuser, config.Dbpass, config.Dbhost, config.Dbport, config.Dbname, config.ExternalLib),
		TableExistsSql: `select count(*) from user_tables where table_name=$1`,
	}
	return newSqlDb(impl)
}

func NewSqliteSqlImpl(config SqlDbConfig) (*SqlDb, error) {
	impl := SqlDbImpl{
		Driver:         "sqlite3",
		Url:            config.Path,
		TableExistsSql: `SELECT count(*) FROM sqlite_master WHERE type='table' AND name=$1`,
	}
	return newSqlDb(impl)
}

type SqlDb struct {
	dbimpl SqlDbImpl
	db     *sqlx.DB
	tx     *sqlx.Tx
}

func newSqlDb(dbimpl SqlDbImpl) (*SqlDb, error) {
	db, err := sqlx.Open(dbimpl.Driver, dbimpl.Url)
	if err != nil {
		return nil, err
	}
	return &SqlDb{dbimpl, db, nil}, nil
}

func (sdb *SqlDb) CopyRow(table *Table, rowNum int, row interface{}) {
	_, err := sdb.tx.NamedExec(table.InsertSql, row)
	if err != nil {
		panic(err)
	}
}

func (sdb *SqlDb) GetRows(table *Table) (*sqlx.Rows, error) {
	return sdb.db.Queryx(table.SelectSql)
}

func (sdb *SqlDb) StartTransaction() error {
	tx, err := sdb.db.Beginx()
	if err != nil {
		return err
	}
	sdb.tx = tx
	return nil
}

func (sdb *SqlDb) TableExists(name string) (bool, error) {
	var exists int
	err := sdb.db.Get(&exists, sdb.dbimpl.TableExistsSql, name)
	return exists > 0, err
}

func (sdb *SqlDb) CreateTable(table *Table) error {
	sql := createTableSql(table.Name, reflect.TypeOf(table.Fields))
	_, err := sdb.db.Exec(sql)
	return err
}

func (sdb *SqlDb) Rollback() error {
	return sdb.tx.Rollback()
}

func (sdb *SqlDb) Commit() error {
	return sdb.tx.Commit()
}

func (sdb *SqlDb) Close() {
	err := sdb.db.Close()
	if err != nil {
		log.Printf("Error closing database connection: %s\n", err)
	}
}
