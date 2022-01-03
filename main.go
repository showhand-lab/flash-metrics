package main

import (
	"database/sql"
	"flag"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"github.com/zhongzc/flash-metrics-write/table"
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

func init() {
	flag.Parse()

	if len(*tidbAddr) == 0 {
		log.Fatal("empty tidb address", zap.String("address", *tidbAddr))
	}

	// Setup
	d, err := sql.Open("mysql", fmt.Sprintf("root@(%s)/test?parseTime=true", *tidbAddr))
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

	for _, stmt := range setupTiflashReplica {
		if _, err = db.Exec(stmt); err != nil {
			log.Fatal("failed to set replica", zap.String("statement", stmt), zap.Error(err))
		}
	}
}

// control
var setupTiflashReplica = []string{
	"ALTER TABLE flash_metrics_data SET TIFLASH REPLICA 1;",
	"ALTER TABLE flash_metrics_index SET TIFLASH REPLICA 1;",
	"ALTER TABLE flash_metrics_update SET TIFLASH REPLICA 1;",
}

func main() {

}
