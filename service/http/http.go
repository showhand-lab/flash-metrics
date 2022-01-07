package http

import (
	"net"
	"net/http"
	"time"

	"github.com/showhand-lab/flash-metrics-storage/remote"
	"github.com/showhand-lab/flash-metrics-storage/store"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

var (
	httpServer *http.Server = nil
)

func ServeHTTP(listener net.Listener, storage store.MetricStorage) {
	mux := http.NewServeMux()
	mux.HandleFunc("/write", remote.WriteHandler(storage))
	mux.HandleFunc("/read", remote.ReadHandler(storage))

	mux.HandleFunc("/api/v1/query", QueryHandler(storage))
	mux.HandleFunc("/api/v1/query_range", QueryRangeHandler(storage))
	// mux.HandleFunc("/match", _)

	mux.HandleFunc("/", DefaultHandler)

	httpServer = &http.Server{Handler: mux}
	if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
		log.Warn("failed to serve http service", zap.Error(err))
	}
}

func StopHTTP() {
	if httpServer == nil {
		return
	}

	now := time.Now()
	log.Info("shutting down http server")
	defer func() {
		log.Info("http server is down", zap.Duration("in", time.Since(now)))
	}()

	if err := httpServer.Close(); err != nil {
		log.Warn("failed to close http server", zap.Error(err))
	}
	httpServer = nil
}
