package service

import (
	"net"

	"github.com/pingcap/log"
	"github.com/showhand-lab/flash-metrics-storage/service/http"
	"go.uber.org/zap"
)

func Init(addr string) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("failed to listen",
			zap.String("address", addr),
			zap.Error(err),
		)
	}

	go http.ServeHTTP(listener)

	log.Info(
		"starting http service",
		zap.String("address", addr),
	)
}

func Stop() {
	http.StopHTTP()
}
