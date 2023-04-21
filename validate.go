package main

import (
	"database/sql"
	"log"
	"os"
)

func validate() (*sql.DB, func()) {
	var dbName string
	if len(os.Args) != 3 {
		log.Fatal("incorrect number of arguments")
	} else {
		key := os.Args[1]
		if key != "--datadir" {
			log.Fatal("datadir argument in mandatory")
		}
		dbName = os.Args[2]
		if _, err := os.Stat(dbName); os.IsNotExist(err) {
			log.Fatal("SQLite database file doesn't exist")
		}
	}
	dsn := dbName + "?_journal=wal"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		log.Fatalf("unable to open sqlite db %v", err)
	}
	if err = db.Ping(); err != nil {
		log.Fatalf("unable to ping sqlite db %v", err)
	}
	return db, func() { db.Close() }
}
