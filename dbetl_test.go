package dbetl

import (
	"log"
	"os"
	"testing"
	"time"

	_ "github.com/godror/godror"
)

type PrecipDay struct {
	SourceCode     string    `db:"SOURCE_CODE" desttype:"varchar(2)"`
	AreaTypeCode   string    `db:"AREA_TYPE_CODE" desttype:"varchar(2)"`
	AreaId         int       `db:"AREA_ID" desttype:"int"`
	EndDate        time.Time `db:"END_DATE" desttype:"date"`
	DayOfWaterYear int       `db:"DAY_OF_WATER_YEAR" desttype:"int"`
	Value          float32   `db:"VALUE" desttype:"double"`
	UnitCode       string    `db:"UNIT_CODE" desttype:"varchar(2)"`
}

func TestEtl(t *testing.T) {
	oraConfig := SqlDbConfig{
		ExternalLib: os.Getenv("INSTANTCLIENT"),
		Dbhost:      os.Getenv("DBHOST"),
		Dbname:      os.Getenv("DBNAME"),
		Dbuser:      os.Getenv("DBUSER"),
		Dbpass:      os.Getenv("DBPASS"),
	}
	sqliteConfig := SqlDbConfig{
		Path: "/Users/rdcrlrsg/Working/crrel/crb/test/crb.db",
	}

	source, err := NewOracleSqlImpl(oraConfig)
	if err != nil {
		log.Fatal(err)
	}

	dest, err := NewSqliteSqlImpl(sqliteConfig)
	if err != nil {
		log.Fatal(err)
	}

	options := TransferOptions{
		CreateTable: true,
		CommitSize:  100,
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
