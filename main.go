package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	stdlog "log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/showhand-lab/flash-metrics-storage/config"
	"github.com/showhand-lab/flash-metrics-storage/scrape"
	"github.com/showhand-lab/flash-metrics-storage/service"
	"github.com/showhand-lab/flash-metrics-storage/store"
	"github.com/showhand-lab/flash-metrics-storage/table"
	"github.com/showhand-lab/flash-metrics-storage/utils/printer"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	_ "github.com/go-sql-driver/mysql"
)

const (
	nmConfigFilePath = "config.file"
	nmAddr           = "address"
	nmTiDBAddr       = "tidb.address"
	nmLogLevel       = "log.level"
	nmLogFile        = "log.file"
	nmCleanup        = "cleanup"
)

var (
	cfgFilePath = flag.String(nmConfigFilePath, "./flashmetrics.yml", "YAML config file path for flashmetrics.")
	cleanup     = flag.Bool(nmCleanup, false, "Whether to cleanup data during shutting down, set for debug")
	tidbAddr    = flag.String(nmTiDBAddr, "127.0.0.1:4000", "The address of TiDB")
	listenAddr  = flag.String(nmAddr, "127.0.0.1:9977", "TCP address to listen for http connections")
	logLevel    = flag.String(nmLogLevel, "info", "Log level")
	logFile     = flag.String(nmLogFile, "", "Log file")
)

func overrideConfig(config *config.FlashMetricsConfig) {
	flag.Visit(func(f *flag.Flag) {
		switch f.Name {
		case nmAddr:
			config.WebConfig.Address = *listenAddr
		case nmTiDBAddr:
			config.TiDBConfig.Address = *tidbAddr
		case nmLogFile:
			config.LogConfig.LogFile = *logFile
		case nmLogLevel:
			config.LogConfig.LogLevel = *logLevel
		}
	})
}

func initLogger(cfg *config.FlashMetricsConfig) error {
	logCfg := &log.Config{
		Level: cfg.LogConfig.LogLevel,
		File:  log.FileLogConfig{Filename: cfg.LogConfig.LogFile},
	}

	logger, p, err := log.InitLogger(logCfg)
	if err != nil {
		return err
	}
	log.ReplaceGlobals(logger, p)
	return nil
}

func initDatabase(cfg *config.FlashMetricsConfig) *sql.DB {
	now := time.Now()
	log.Info("setting up database")
	defer func() {
		log.Info("init database done", zap.Duration("in", time.Since(now)))
	}()

	db, err := sql.Open("mysql", fmt.Sprintf("root@(%s)/test", cfg.TiDBConfig.Address))
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
	defer func() {
		log.Info("close database done", zap.Duration("in", time.Since(now)))
	}()

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

func waitForSigterm() os.Signal {
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

func main() {
	flag.Parse()

	flashMetricsConfig, err := config.LoadConfig(*cfgFilePath, overrideConfig)
	if err != nil {
		// logger isn't initialized, need to use stdlog
		stdlog.Fatalf("failed to load config file, config.file: %s", *cfgFilePath)
	}
	err = initLogger(flashMetricsConfig)
	if err != nil {
		// failed to initialize logger, need to use stdlog
		stdlog.Fatalf("failed to init logger, err: %s", err.Error())
	}

	printer.PrintFlashMetricsStorageInfo()

	if len(flashMetricsConfig.WebConfig.Address) == 0 {
		log.Fatal("empty listen address", zap.String("listen-address", flashMetricsConfig.WebConfig.Address))
	}

	db := initDatabase(flashMetricsConfig)
	defer closeDatabase(db)

	storage := store.NewDefaultMetricStorage(db)

	service.Init(flashMetricsConfig, storage)
	defer service.Stop()

	scrapeCtx, cancelScrape := context.WithCancel(context.Background())
	scrape.Init(flashMetricsConfig, scrapeCtx, storage)
	defer cancelScrape()

	sig := waitForSigterm()
	log.Info("received signal", zap.String("sig", sig.String()))
}
