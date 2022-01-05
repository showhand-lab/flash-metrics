package service

import (
	"github.com/showhand-lab/flash-metrics-storage/config"
	"net"

	"github.com/showhand-lab/flash-metrics-storage/service/http"
	"github.com/showhand-lab/flash-metrics-storage/store"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func Init(cfg *config.FlashMetricsConfig, storage store.MetricStorage) {
	addr := cfg.WebConfig.Address
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("failed to listen",
			zap.String("address", addr),
			zap.Error(err),
		)
	}

	go http.ServeHTTP(listener, storage)

	log.Info(
		"starting http service",
		zap.String("address", addr),
	)
}

func Stop() {
	http.StopHTTP()
}
