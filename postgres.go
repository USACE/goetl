package dbetl

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"log"

	"github.com/jackc/pgx/v4"
)

const defaultBatchSize = 100

type PostgresConfig struct {
	BatchSize int
	SqlDbConfig
}

type Connx struct {
	pgx             *pgx.Conn
	namedStatements map[string]NamedStatement
	batch           *pgx.Batch
}

func (c *Connx) NamedExec(sql string, data interface{}) {
	h := getHash(sql)
	ns, ok := c.namedStatements[h]
	if !ok {
		ns = NewNamedStatement(sql, &data)
	}
	c.batch.Queue(ns.ParamSql, ns.ParamArray(&data))
}

type PostgresDb struct {
	db        *Connx
	batchSize int
}

func NewPostgresDb(c PostgresConfig) (*PostgresDb, error) {
	conn, err := pgx.Connect(context.Background(), c.ToDsn())
	if err != nil {
		log.Fatalf("Unable to connect to database: %v\n", err)
	}

	pg := PostgresDb{
		db: &Connx{
			pgx:   conn,
			batch: &pgx.Batch{},
		},
		batchSize: defaultBatchSize,
	}

	if c.BatchSize > 0 {
		pg.batchSize = c.BatchSize
	}
	return &pg, nil
}

//func (pdb *PostgresDb) GetRows(table *Table) ()

func getHash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}
