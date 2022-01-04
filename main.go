package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"github.com/showhand-lab/flash-metrics-storage/config"
	"github.com/showhand-lab/flash-metrics-storage/service"
	"github.com/showhand-lab/flash-metrics-storage/store"
	"github.com/showhand-lab/flash-metrics-storage/table"
	"github.com/showhand-lab/flash-metrics-storage/utils/printer"
	"go.uber.org/zap"
)

const (
	nmTiDBAddr       = "tidb.address"
	nmAddr           = "address"
	nmConfigFilePath = "config.file"
)

// flags
var (
	cfgFilePath = flag.String(nmConfigFilePath, "./flashmetrics.yml", "YAML config file path for flashmetrics.")
	tidbAddr    = flag.String(nmTiDBAddr, "", "The address of TiDB")
	listenAddr  = flag.String(nmAddr, "", "TCP address to listen for http connections")
)

// global variables
var (
	db *sql.DB

	mstore store.MetricStorage
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

func closeDatabase() {
	if db != nil {
		if err := db.Close(); err != nil {
			log.Warn("failed to close database", zap.Error(err))
		}
		db = nil
	}
}

func initStore() {
	mstore = store.NewDefaultMetricStorage(db)
}

func closeStore() {}

func main() {
	flag.Parse()

	printer.PrintFlashMetricsStorageInfo()

	cfg, err := config.LoadConfig(*cfgFilePath)
	if err != nil {
		log.Fatal("fail to load config file ", zap.String("config.file", *cfgFilePath))
	}
	log.Info("targets ", zap.String("targets", cfg.ScrapeConfig[0].JobName))

	initDatabase()
	defer closeDatabase()

	initStore()
	defer closeStore()

	if len(*listenAddr) == 0 {
		log.Fatal("empty listen address", zap.String("listen-address", *listenAddr))
	}
	service.Init(*listenAddr)
	defer service.Stop()

	sig := WaitForSigterm()
	log.Info("received signal", zap.String("sig", sig.String()))
}

func WaitForSigterm() os.Signal {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	for {
		sig := <-ch
		if sig == syscall.SIGHUP {
			// Prevent from the program stop on SIGHUP
			continue
		}
		return sig
	}
}
