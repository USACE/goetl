package goetl

import (
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	_ "github.com/godror/godror"
	_ "github.com/mattn/go-sqlite3"
)

type PrecipDay struct {
	ID             int       `db:"id" dbid:"AUTOINCREMENT" desttype:"integer PRIMARY KEY AUTOINCREMENT"`
	SourceCode     string    `db:"SOURCE_CODE" desttype:"varchar(2)"`
	AreaTypeCode   string    `db:"AREA_TYPE_CODE" desttype:"varchar(2)"`
	AreaId         int       `db:"AREA_ID" desttype:"integer"`
	EndDate        time.Time `db:"END_DATE" desttype:"date"`
	DayOfWaterYear int       `db:"DAY_OF_WATER_YEAR" desttype:"int"`
	Value          float32   `db:"VALUE" desttype:"double precision"`
	UnitCode       string    `db:"UNIT_CODE" desttype:"varchar(2)"`
}

func TestSqlite(t *testing.T) {

	/*
		ist, err := InsertSql("precip_day_hour", reflect.TypeOf(PrecipDay{}))
		if err != nil {
			log.Fatal(err)
		}
		log.Println(ist)
	*/
	oraConfig := DbConfig{
		ExternalLib: os.Getenv("INSTANTCLIENT"),
		Dbhost:      os.Getenv("DBHOST"),
		Dbname:      os.Getenv("DBNAME"),
		Dbuser:      os.Getenv("DBUSER"),
		Dbpass:      os.Getenv("DBPASS"),
	}
	sqliteConfig := DbConfig{
		Path: "/Users/rdcrlrsg/Working/crrel/crb/test/crb.db",
	}

	source, err := NewOracleSqlImpl(oraConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer source.Close()

	dest, err := NewSqliteSqlImpl(sqliteConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer dest.Close()

	options := TransferOptions{
		CreateTable: true,
		CommitSize:  100,
	}

	table := Table{
		Name:      "precip_day",
		SelectSql: "select * from (select * from precip_day_new) t1 where rownum<1000",
		//InsertSql: "insert into precip_day values (:SOURCE_CODE,:AREA_TYPE_CODE,:AREA_ID,:END_DATE,:DAY_OF_WATER_YEAR,:VALUE,:UNIT_CODE)",

		Fields: &PrecipDay{},
	}

	etl := ETL{source, dest, options}

	err = etl.Transfer(&table)
	if err != nil {
		log.Println(err)
	}
}

func TestPostgres(t *testing.T) {
	oraConfig := DbConfig{
		ExternalLib: os.Getenv("INSTANTCLIENT"),
		Dbhost:      os.Getenv("DBHOST"),
		Dbname:      os.Getenv("DBNAME"),
		Dbuser:      os.Getenv("DBUSER"),
		Dbpass:      os.Getenv("DBPASS"),
	}

	pgPort, err := strconv.Atoi(os.Getenv("PGDBPORT"))
	if err != nil {
		log.Fatal(err)
	}

	/*
		pgConfig := PostgresConfig{
			BatchSize: 100,
			DbConfig: DbConfig{
				Dbhost: os.Getenv("PGDBHOST"),
				Dbname: os.Getenv("PGDBNAME"),
				Dbuser: os.Getenv("PGDBUSER"),
				Dbpass: os.Getenv("PGDBPASS"),
				Dbport: pgPort,
			},
		}
	*/

	pgConfig := DbConfig{
		Dbhost: os.Getenv("PGDBHOST"),
		Dbname: os.Getenv("PGDBNAME"),
		Dbuser: os.Getenv("PGDBUSER"),
		Dbpass: os.Getenv("PGDBPASS"),
		Dbport: pgPort,
	}

	source, err := NewOracleSqlImpl(oraConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer source.Close()

	dest, err := NewPostgresDb(pgConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer dest.Close()

	options := TransferOptions{
		CreateTable: false,
		CommitSize:  10,
	}

	table := Table{
		Name:      "precip_day",
		SelectSql: "select * from (select * from precip_day_new) t1 where rownum<100",
		InsertSql: "insert into precip_day values (:SOURCE_CODE,:AREA_TYPE_CODE,:AREA_ID,:END_DATE,:DAY_OF_WATER_YEAR,:VALUE,:UNIT_CODE)",
		Fields:    PrecipDay{},
	}

	etl := ETL{source, dest, options}

	err = etl.Transfer(&table)
	if err != nil {
		log.Println(err)
	}
}
