package main

import (
	"database/sql"
	"flag"
	"fmt"
	stdlog "log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pingcap/log"
	"github.com/showhand-lab/flash-metrics-storage/service"
	"github.com/showhand-lab/flash-metrics-storage/store"
	"github.com/showhand-lab/flash-metrics-storage/table"
	"github.com/showhand-lab/flash-metrics-storage/utils/printer"
	"go.uber.org/zap"
)

const (
	nmTiDBAddr = "tidb.address"
	nmLogLevel = "log.level"
	nmAddr     = "address"
	nmCleanup  = "cleanup"
)

// flags
var (
	tidbAddr   = flag.String(nmTiDBAddr, "127.0.0.1:4000", "The address of TiDB")
	listenAddr = flag.String(nmAddr, "127.0.0.1:9977", "TCP address to listen for http connections")
	logLevel   = flag.String(nmLogLevel, "info", "Log level")
	cleanup    = flag.Bool(nmCleanup, false, "Whether to cleanup data during shutting down, set for debug")
)

// global variables
var (
	db *sql.DB
)

func initLog() {
	cfg := &log.Config{Level: *logLevel}
	logger, p, err := log.InitLogger(cfg)
	if err != nil {
		stdlog.Fatalf("failed to init logger, err: %s", err)
	}
	log.ReplaceGlobals(logger, p)
}

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
		if *cleanup {
			for _, stmt := range []string{table.DropData, table.DropUpdate, table.DropIndex, table.DropMeta} {
				if _, err := db.Exec(stmt); err != nil {
					log.Fatal("failed to set replica", zap.String("statement", stmt), zap.Error(err))
				}
			}
		}

		if err := db.Close(); err != nil {
			log.Warn("failed to close database", zap.Error(err))
		}
		db = nil
	}
}

func main() {
	flag.Parse()

	printer.PrintFlashMetricsStorageInfo()

	initLog()

	initDatabase()
	defer closeDatabase()

	mstore := store.NewDefaultMetricStorage(db)

	if len(*listenAddr) == 0 {
		log.Fatal("empty listen address", zap.String("listen-address", *listenAddr))
	}
	service.Init(*listenAddr, mstore)
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
