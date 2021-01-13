package dbetl

import (
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/jmoiron/sqlx"
)

type TransferOptions struct {
	CreateTable bool
	CommitSize          int
	BatchSize           int
}

type Source interface {
	GetRows(table *Table) (*sqlx.Rows, error)
}

type Destination interface {
	StartTransaction() (*sqlx.Tx, error)
	TableExists(name string) (bool, error)
	CreateTable(table *Table) error
}

type Table struct {
	Name      string
	SelectSql string
	InsertSql string
	Fields    interface{}
}

type ETL struct {
	source  Source
	dest    Destination
	options TransferOptions
}

//type CopyRowsFunction func(row *sqlx.Row) error
//type GetRowsFunction func(db sqlx.DB, table Table)

func (etl *ETL) Transfer(table *Table) error {
	if exists, err := etl.dest.TableExists(table.Name); !exists && etl.options.CreateTable {
		if err != nil {
			return err
		}
		log.Printf("Table does not exist in destination.  Creating table %s", table.Name)
		err := etl.dest.CreateTable(table)
		if err != nil {
			return err
		}
	}
	return etl.copyData(table)
}

func (etl *ETL) copyData(table *Table) (err error) {
	rows, err := etl.source.GetRows(table)
	if err != nil {
		return err
	}
	typ := reflect.TypeOf(table.Fields)
	typeP := reflect.New(typ).Elem().Addr()
	structRef := typeP.Interface()
	var i int = 0
	tx, err := etl.dest.StartTransaction()
	if err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			txerr := tx.Rollback()
			if txerr != nil {
				log.Printf("Unable to rollback from transaction: %s", txerr)
			}
			err = r.(error)
		} else {
			txerr := tx.Commit()
			if txerr != nil {
				log.Printf("Unable to commit transaction: %s", txerr)
			}
			err = nil
		}
	}()
	for rows.Next() {
		err = rows.StructScan(structRef)
		if err != nil {
			return err
		}
		if i%etl.options.CommitSize == 0 {
			if tx != nil {
				err = tx.Commit()
				if err != nil {
					return err
				}
			}
			tx, err = etl.dest.StartTransaction()
			if err != nil {
				return err
			}
			etl.copyRow(table, tx, i, structRef)
		}
	}
	return nil
}

func (etl *ETL) copyRow(table *Table, tx *sqlx.Tx, rowNum int, row interface{}) {
	_, err := tx.NamedExec(table.InsertSql, row)
	if err != nil {
		panic(err)
	}
}

func createTableSql(tablename string, t reflect.Type) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("create table %s (", tablename))
	for i := 0; i < t.NumField(); i++ {
		if dbfield, ok := t.Field(i).Tag.Lookup("db"); ok {
			if dbtype, ok := t.Field(i).Tag.Lookup("desttype"); ok {
				if i > 0 {
					builder.WriteString(",")
				}
				builder.WriteString(fmt.Sprintf("%s %s", dbfield, dbtype))
			}
		}
	}
	builder.WriteString(")")
	return builder.String()
}
