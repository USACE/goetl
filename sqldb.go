package goetl

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"reflect"

	"github.com/georgysavva/scany/sqlscan"
)

func (c *DbConfig) ToDsn() string {
	return fmt.Sprintf(`user=%s password=%s host=%s port=%d dbname=%s`,
		c.Dbuser, c.Dbpass, c.Dbhost, c.Dbport, c.Dbname)
}

//@TODO verify that the Next method is returning a reference from the Rows interface
type SqldbRows struct {
	rows *sql.Rows
}

func (sr SqldbRows) Next() bool {
	return sr.rows.Next()
}

func (sr SqldbRows) StructScan(ref interface{}) error {
	return sqlscan.ScanRow(ref, sr.rows)
}

func (sr SqldbRows) Close() {
	sr.rows.Close()
}

type SqlDbImpl struct {
	Driver           string
	Url              string
	TableExistsSql   string
	TemplateFunction ParameterTemplateFunction
}

func NewOracleSqlImpl(config DbConfig) (*SqlDb, error) {
	port := 1521
	if config.Dbport != 0 {
		port = config.Dbport
	}
	impl := SqlDbImpl{
		Driver: "godror",
		Url: fmt.Sprintf(`user="%s" password="%s" connectString="%s:%d/%s" libDir="%s"`,
			config.Dbuser, config.Dbpass, config.Dbhost, port, config.Dbname, config.ExternalLib),
		TableExistsSql: `select count(*) from user_tables where table_name=$1`,
		TemplateFunction: func(field string, i int) string {
			return fmt.Sprintf(":%s", field)
		},
	}
	return newSqlDb(impl)
}

func NewPostgresSqlImpl(config DbConfig) (*SqlDb, error) {
	if config.Dbport == 0 {
		config.Dbport = 5432
	}
	impl := SqlDbImpl{
		Driver:         "pgx",
		Url:            config.ToDsn(),
		TableExistsSql: pgTableExists,
		TemplateFunction: func(field string, i int) string {
			return fmt.Sprintf("$%d", i)
		},
	}
	return newSqlDb(impl)
}

func NewSqliteSqlImpl(config DbConfig) (*SqlDb, error) {
	impl := SqlDbImpl{
		Driver:         "sqlite3",
		Url:            config.Path,
		TableExistsSql: `SELECT count(*) FROM sqlite_master WHERE type='table' AND name=$1`,
		TemplateFunction: func(field string, i int) string {
			return fmt.Sprintf("$%d", i)
		},
	}
	return newSqlDb(impl)
}

type SqlDb struct {
	dbimpl SqlDbImpl
	db     *sql.DB
	tx     *sql.Tx
}

func newSqlDb(dbimpl SqlDbImpl) (*SqlDb, error) {
	db, err := sql.Open(dbimpl.Driver, dbimpl.Url)
	if err != nil {
		return nil, err
	}
	return &SqlDb{dbimpl, db, nil}, nil
}

func (sdb *SqlDb) CopyRow(table *Table, rowNum int, row interface{}) {
	ns, err := NewNamedStatement(sdb.dbimpl.Driver, sdb.dbimpl.TemplateFunction, table)
	if err != nil {
		panic(err)
	}
	params := ns.ParamArray(row)
	_, err = sdb.tx.Exec(ns.ParamSql, params...)
	if err != nil {
		panic(err)
	}
}

func (sdb *SqlDb) GetRows(table *Table) (Rows, error) {
	rows, err := sdb.db.Query(table.SelectSql)
	return SqldbRows{rows}, err

}

func (sdb *SqlDb) StartTransaction() error {
	tx, err := sdb.db.Begin()
	if err != nil {
		return err
	}
	sdb.tx = tx
	return nil
}

func (sdb *SqlDb) TableExists(schema string, name string) (bool, error) {
	var exists int
	var row *sql.Row
	if sdb.dbimpl.Driver == "pgx" {
		row = sdb.db.QueryRowContext(context.Background(), sdb.dbimpl.TableExistsSql, schema, name)
	} else {
		row = sdb.db.QueryRowContext(context.Background(), sdb.dbimpl.TableExistsSql, name)
	}
	err := row.Scan(&exists)
	return exists > 0, err
}

func (sdb *SqlDb) CreateTable(table *Table) error {
	sql := CreateTableSql(table.Name, reflect.TypeOf(table.Fields).Elem())
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
