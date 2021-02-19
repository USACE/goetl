package goetl

import (
	"fmt"
)

type DevNullDb struct{}

func NewDevNullDb() (*DevNullDb, error) {

	dn := DevNullDb{}
	return &dn, nil
}

func (dndb *DevNullDb) GetRows(table *Table) (Rows, error) { return SqldbRows{}, nil }

func (dndb *DevNullDb) Close() {}

func (dndb *DevNullDb) StartTransaction() error { return nil }

func (dndb *DevNullDb) Rollback() error { return nil }

func (dndb *DevNullDb) Commit() error {
	fmt.Println("Commit Called")
	return nil
}

func (dndb *DevNullDb) TableExists(schema string, name string) (bool, error) {
	return true, nil
}

func (dndb *DevNullDb) CopyRow(table *Table, rowNum int, row interface{}) {
	//pdb.db.NamedExec("pgx", table, row)
}

func (dndb *DevNullDb) CreateTable(table *Table) error { return nil }
