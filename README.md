# Golang Database ETL Library
This is a simple library to facilitate data transfers in go programs between different data stores.  It is currently designed around the database/sql interface, but the long term intent is to support any kind of data transfer.  This code is in an alpha state and the API is not stable. 

To use the library, 
 - create a struct with both a db tag and a desttype tag (if you would like the tables to be automatically created in the destination). 
 - create connections to the source and destination database
 - define the transfer options
   - CreateTable: whether the tools should create the table if it does not exists.  If the table does exists, the transfer will append any data to the existing table.
   - CommitSize: the transaction commit size.
   - BatchSize: this is currently unsupported in most golang db drivers, but does exists for the pgx postgres driver.  Batching commands reduces the number of networks calls and can significantly improve insert performance.
 - Create the Table struct for the table to transfer
   - Name: name of the table
     - For selects, this is used for the query if a select statement was not provided.
     - For table creation, this will be the name of the new table.
     - For inserts, this will be used if an insert query was not provided.
   - SelectSql: user supplied SQL for the select if you do not want if autogenerated from struct tags
   - InsertSql: user supplied SQL for inserts if you do not want the insert autogenerated from struct tags
   - Fields: Struct that will be used to scan result data into
 - Create an ETL instance with the source, destination, and transfer options
 - Transfer as many tables as you need using the etl.Transfer function
     
```golang

type PrecipDay struct {
	SourceCode     string    `db:"SOURCE_CODE" desttype:"varchar(2)"`
	AreaTypeCode   string    `db:"AREA_TYPE_CODE" desttype:"varchar(2)"`
	AreaId         int       `db:"AREA_ID" desttype:"int"`
	EndDate        time.Time `db:"END_DATE" desttype:"date"`
	DayOfWaterYear int       `db:"DAY_OF_WATER_YEAR" desttype:"int"`
	Value          float32   `db:"VALUE" desttype:"double"`
	UnitCode       string    `db:"UNIT_CODE" desttype:"varchar(2)"`
}

oraConfig := OracleConfig{
    InstantClient: os.Getenv("INSTANTCLIENT"),
    Dbhost:        os.Getenv("DBHOST"),
    Dbname:        os.Getenv("DBNAME"),
    Dbuser:        os.Getenv("DBUSER"),
    Dbpass:        os.Getenv("DBPASS"),
}

sqliteConfig := SqliteConfig{
    Dbpath: "/Users/rdcrlrsg/Working/crrel/crb/test/crb.db",
}

source, err := NewOracleDb(oraConfig)
if err != nil {
    log.Fatal(err)
}

dest, err := NewSqliteDb(sqliteConfig)
if err != nil {
    log.Fatal(err)
}

options := TransferOptions{
    CreateTable: true,
    CommitSize:  100,
    BatchSize:   100,
}

table := Table{
    Name:      "precip_day",
    SelectSql: "select * from (select * from precip_day_new) t1 where rownum<100",
    InsertSql: "insert into precip_day values (:SOURCE_CODE,:AREA_TYPE_CODE,:AREA_ID,:END_DATE,:DAY_OF_WATER_YEAR,:VALUE,:UNIT_CODE)",
    Fields:    PrecipDay{},
}

etl := ETL{source, dest, options}

err = etl.Transfer(&table)
tif err != nil {
    log.Println(err)
}
```

This initial version includes support for Oracle (db source only) and Sqlite.

The library uses:
 - godror for the Oracle driver
 - go-sqlite3 for sqlite
 - pgx for postgres (not implemented)

 TODO
  - Generate select sql from struct
  - Generate insert sql from struct
  - Allow user to override Read and Write methods with their own functions