package goetl

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
)

type TransferOptions struct {
	CreateTable bool
	CommitSize  int
}

type DbConfig struct {
	Path        string
	Dbuser      string
	Dbpass      string
	Dbname      string
	Dbhost      string
	Dbport      int
	ExternalLib string
}

type Rows interface {
	Next() bool
	Close()
	StructScan(ref interface{}) error
}

type Source interface {
	GetRows(table *Table) (Rows, error)
	Close()
}

type Destination interface {
	StartTransaction() error
	Rollback() error
	Commit() error
	TableExists(schema string, name string) (bool, error)
	CreateTable(table *Table) error
	CopyRow(table *Table, rownum int, row interface{})
	Close()
}

type Table struct {
	Name      string
	Schema    string //optional
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
	if exists, err := etl.dest.TableExists(table.Schema, table.Name); !exists && etl.options.CreateTable {
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
	defer rows.Close()
	typ := reflect.TypeOf(table.Fields).Elem()
	typeP := reflect.New(typ).Elem().Addr()
	structRef := typeP.Interface()
	var i int = 0
	err = etl.dest.StartTransaction()
	if err != nil {
		return err
	}
	defer func() {
		if r := recover(); r != nil {
			txerr := etl.dest.Rollback()
			if txerr != nil {
				log.Printf("Unable to rollback from transaction: %s", txerr)
			}
			err = r.(error)
		} else {
			txerr := etl.dest.Commit()
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
			err = etl.dest.Commit()
			if err != nil {
				return err
			}
			err = etl.dest.StartTransaction()
			if err != nil {
				return err
			}
			etl.dest.CopyRow(table, i, structRef)
		}
	}
	return nil
}

func CreateTableSql(tablename string, t reflect.Type) string {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("create table %s (", tablename))
	fieldnum := 0
	for i := 0; i < t.NumField(); i++ {
		if dbfield, ok := t.Field(i).Tag.Lookup("db"); ok {
			if dbtype, ok := t.Field(i).Tag.Lookup("desttype"); ok {
				if fieldnum > 0 {
					builder.WriteString(",")
				}
				builder.WriteString(fmt.Sprintf("%s %s", dbfield, dbtype))
				fieldnum++
			}
		}
	}
	builder.WriteString(")")
	return builder.String()
}

func InsertSql(tablename string, t reflect.Type) (string, error) {
	var fieldBuild strings.Builder
	var valsBuild strings.Builder
	fieldcount := 0
	for i := 0; i < t.NumField(); i++ {
		if dbfield, ok := t.Field(i).Tag.Lookup("db"); ok {
			if fieldcount > 0 {
				fieldBuild.WriteRune(',')
				valsBuild.WriteRune(',')
			}
			if idtype, ok := t.Field(i).Tag.Lookup("id"); ok {
				if idtype != "AUTOINCREMENT" {
					if idsequence, ok := t.Field(i).Tag.Lookup("idsequence"); ok {
						fieldBuild.WriteString(dbfield)
						valsBuild.WriteString(idsequence)
						fieldcount++
					} else {
						return "", errors.New("Invalid id.  Sequence type must have an 'idsequence' tag")
					}
				}
			} else {
				fieldBuild.WriteString(dbfield)
				valsBuild.WriteString(":" + dbfield)
				fieldcount++
			}
		}
	}
	return fmt.Sprintf("insert into %s (%s) values (%s)", tablename, fieldBuild.String(), valsBuild.String()), nil
}

//id "identity|sequence" idsequence ""
