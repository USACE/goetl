package goetl

import (
	"context"
	"fmt"
	"log"
	"reflect"

	"github.com/georgysavva/scany/pgxscan"
	"github.com/jackc/pgx/v4"
)

var pgTableExists string = `SELECT count(*) FROM information_schema.tables WHERE  table_schema = $1 AND table_name = $2`

var pgBindTemplateFunction = func(field string, i int) string {
	return fmt.Sprintf("$%d", i)
}

type Connx struct {
	pgx   *pgx.Conn
	batch *pgx.Batch
}

func (c *Connx) NamedExec(driver string, table *Table, data interface{}) {
	ns, err := NewNamedStatement(
		driver,
		func(field string, i int) string {
			return fmt.Sprintf("$%d", i)
		},
		table,
	)
	if err != nil {
		panic(err)
	}
	copy := copyElem(data)
	params := ns.ParamArray(copy)
	c.batch.Queue(ns.ParamSql, params...)
}

func (c *Connx) FlushBatch() {
	br := c.pgx.SendBatch(context.Background(), c.batch)
	defer br.Close()
	c.batch = &pgx.Batch{}
}

type PgxRows struct {
	rows  pgx.Rows
	table *Table
}

func (pgr PgxRows) Next() bool {
	return pgr.rows.Next()
}

func (pgr PgxRows) StructScan(ref interface{}) error {
	return pgxscan.ScanRow(ref, pgr.rows)
}

func (pgr PgxRows) Close() {
	pgr.rows.Close()
}

type PostgresDb struct {
	db *Connx
}

func NewPostgresDb(c DbConfig) (*PostgresDb, error) {
	conn, err := pgx.Connect(context.Background(), c.ToDsn())
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}

	pg := PostgresDb{
		db: &Connx{
			pgx:   conn,
			batch: &pgx.Batch{},
		},
	}
	return &pg, nil
}

func (pdb *PostgresDb) GetRows(table *Table) (Rows, error) {
	rows, err := pdb.db.pgx.Query(context.Background(), table.SelectSql)
	return PgxRows{rows, table}, err
}

func (pdb *PostgresDb) Close() {
	err := pdb.db.pgx.Close(context.Background())
	if err != nil {
		log.Printf("Unable to close connection: %s\n", err)
	}
}

func (pdb *PostgresDb) StartTransaction() error {
	pdb.db.batch = &pgx.Batch{}
	return nil
}

func (pdb *PostgresDb) Rollback() error {
	pdb.db.batch = &pgx.Batch{} //using implicit batch transactions.  Just reset the batch
	return nil
}

func (pdb *PostgresDb) Commit() error {
	pdb.db.FlushBatch()
	return nil //@TODO need to scan batch for errors and return 1st errror
}

func (pdb *PostgresDb) TableExists(schema string, name string) (bool, error) {
	var exists int
	row := pdb.db.pgx.QueryRow(context.Background(), pgTableExists, schema, name)
	err := row.Scan(&exists)
	return exists > 0, err
}

func (pdb *PostgresDb) CopyRow(table *Table, rowNum int, row interface{}) {
	pdb.db.NamedExec("pgx", table, row)
}

func (pdb *PostgresDb) CreateTable(table *Table) error {
	sql := CreateTableSql(table.Name, reflect.TypeOf(table.Fields).Elem())
	_, err := pdb.db.pgx.Exec(context.Background(), sql)
	return err
}
