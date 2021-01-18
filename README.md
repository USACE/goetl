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
```

This initial version includes support for Oracle (db source only), Sqlite, and Postgres

The library uses:
 - godror for the Oracle driver
 - go-sqlite3 for sqlite
 - pgx for postgres
 - scany for struct scanning

 TODO
  - improve named statement caching (use combination of driver and statement)
  - Allow user to override Read and Write methods with their own functions
  - add option for case insenstitive named statement parameter matching
  - generate sql update statements from struct
  - remove nesting of DbConfig in the Postgresconfig
  - unify batch and commit size
  - enumerate and return errors from postgres batched statements
  - review error handling
