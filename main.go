package main

import (
	"database/sql"
	"flag"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"github.com/showhand-lab/flash-metrics-storage/table"
	"github.com/showhand-lab/flash-metrics-storage/utils/printer"
	"go.uber.org/zap"
)

const (
	nmTiDBAddr = "tidb.address"
)

// flags
var (
	tidbAddr = flag.String(nmTiDBAddr, "", "The address of TiDB")
)

// global variables
var (
	db *sql.DB
)

func initDatabase() {
	if len(*tidbAddr) == 0 {
		log.Fatal("empty tidb address", zap.String("address", *tidbAddr))
	}

	// Setup
	d, err := sql.Open("mysql", fmt.Sprintf("root@(%s)/test", *tidbAddr))
	if err != nil {
		log.Fatal("failed to open db", zap.Error(err))
	}
	d.SetConnMaxLifetime(time.Minute * 3)
	d.SetMaxOpenConns(10)
	d.SetMaxIdleConns(10)
	db = d

	for _, stmt := range []string{table.CreateMeta, table.CreateIndex, table.CreateUpdate, table.CreateData} {
		if _, err = db.Exec(stmt); err != nil {
			log.Fatal("failed to create table", zap.String("statement", stmt), zap.Error(err))
		}
	}

	for _, stmt := range []string{table.AlterTiflashIndex, table.AlterTiflashUpdate, table.AlterTiflashData} {
		if _, err = db.Exec(stmt); err != nil {
			log.Fatal("failed to set replica", zap.String("statement", stmt), zap.Error(err))
		}
	}
}

func main() {
	flag.Parse()

	printer.PrintFlashMetricsStorageInfo()

	initDatabase()
}
