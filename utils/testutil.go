package utils

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/showhand-lab/flash-metrics-storage/table"
)

func PingTiDB() error {
	db, err := sql.Open("mysql", "root@(127.0.0.1:4000)/")
	if err != nil {
		return err
	}

	err = db.Ping()
	if err != nil {
		return err
	}

	return db.Close()
}

func SetupDB(dbName string) (*sql.DB, error) {
	db, err := sql.Open("mysql", "root@(127.0.0.1:4000)/")
	defer func() {
		if err != nil {
			_ = db.Close()
		}
	}()
	if err != nil {
		return nil, err
	}

	if _, err = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName)); err != nil {
		return nil, err
	}
	if _, err = db.Exec(fmt.Sprintf("USE %s", dbName)); err != nil {
		return nil, err
	}

	for _, stmt := range []string{table.CreateMeta, table.CreateIndex, table.CreateUpdate, table.CreateData} {
		if _, err = db.Exec(stmt); err != nil {
			return nil, err
		}
	}

	return db, nil
}

func TearDownDB(dbName string, db *sql.DB) error {
	_, err := db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
	if err != nil {
		_ = db.Close()
		return err
	}
	return db.Close()
}
