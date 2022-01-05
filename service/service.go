package service

import (
	"net"

	"github.com/pingcap/log"
	"github.com/showhand-lab/flash-metrics-storage/service/http"
	"github.com/showhand-lab/flash-metrics-storage/store"
	"go.uber.org/zap"
)

func Init(addr string, mstore store.MetricStorage) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("failed to listen",
			zap.String("address", addr),
			zap.Error(err),
		)
	}

	go http.ServeHTTP(listener, mstore)

	log.Info(
		"starting http service",
		zap.String("address", addr),
	)
}

func Stop() {
	http.StopHTTP()
}
