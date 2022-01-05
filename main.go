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
	nmAddr     = "address"
	nmTiDBAddr = "tidb.address"

	nmLogLevel = "log.level"
	nmLogPath  = "log.path"

	nmCleanup = "cleanup"
)

var (
	tidbAddr   = flag.String(nmTiDBAddr, "127.0.0.1:4000", "The address of TiDB")
	listenAddr = flag.String(nmAddr, "127.0.0.1:9977", "TCP address to listen for http connections")
	logLevel   = flag.String(nmLogLevel, "info", "Log level")
	logPath    = flag.String(nmLogPath, "", "Log path")
	cleanup    = flag.Bool(nmCleanup, false, "Whether to cleanup data during shutting down, set for debug")
)

func initLogger() {
	cfg := &log.Config{Level: *logLevel}

	if *logPath != "" {
		cfg.File.Filename = *logPath
	}

	logger, p, err := log.InitLogger(cfg)
	if err != nil {
		stdlog.Fatalf("failed to init logger, err: %s", err)
	}
	log.ReplaceGlobals(logger, p)
}

func initDatabase() *sql.DB {
	now := time.Now()
	log.Info("setting up database")
	defer log.Info("init database done", zap.Duration("in", time.Since(now)))

	db, err := sql.Open("mysql", fmt.Sprintf("root@(%s)/test", *tidbAddr))
	if err != nil {
		log.Fatal("failed to open db", zap.Error(err))
	}
	db.SetConnMaxLifetime(time.Minute * 3)
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(10)

	for _, stmt := range []string{table.CreateMeta, table.CreateIndex, table.CreateUpdate, table.CreateData} {
		if _, err = db.Exec(stmt); err != nil {
			log.Fatal("failed to create table", zap.String("statement", stmt), zap.Error(err))
		}
	}
	log.Info("create tables successfully")

	for _, stmt := range []string{table.AlterTiflashIndex, table.AlterTiflashUpdate, table.AlterTiflashData} {
		if _, err = db.Exec(stmt); err != nil {
			log.Warn("failed to set replica", zap.String("statement", stmt), zap.Error(err))
		}
	}

	return db
}

func closeDatabase(db *sql.DB) {
	now := time.Now()
	log.Info("closing database")
	defer log.Info("close database done", zap.Duration("in", time.Since(now)))

	if *cleanup {
		for _, stmt := range []string{table.DropData, table.DropUpdate, table.DropIndex, table.DropMeta} {
			if _, err := db.Exec(stmt); err != nil {
				log.Warn("failed to drop table", zap.String("statement", stmt), zap.Error(err))
			}
		}
		log.Info("clean up database successfully")
	}

	if err := db.Close(); err != nil {
		log.Warn("failed to close database", zap.Error(err))
	}
}

func main() {
	flag.Parse()
	initLogger()

	printer.PrintFlashMetricsStorageInfo()
	if len(*listenAddr) == 0 {
		log.Fatal("empty listen address", zap.String("listen-address", *listenAddr))
	}

	db := initDatabase()
	defer closeDatabase(db)

	storage := store.NewDefaultMetricStorage(db)
	service.Init(*listenAddr, storage)
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
