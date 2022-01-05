package http

import (
	"net"
	"net/http"

	"github.com/pingcap/log"
	"github.com/showhand-lab/flash-metrics-storage/remote"
	"github.com/showhand-lab/flash-metrics-storage/store"
	"go.uber.org/zap"
)

var (
	httpServer *http.Server = nil
)

func ServeHTTP(listener net.Listener, mstore store.MetricStorage) {
	mux := http.NewServeMux()
	mux.HandleFunc("/write", remote.WriteHandler(mstore))
	mux.HandleFunc("/read", remote.ReadHandler(mstore))

	httpServer = &http.Server{Handler: mux}
	if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
		log.Warn("failed to serve http service", zap.Error(err))
	}
}

func StopHTTP() {
	if httpServer == nil {
		return
	}

	log.Info("shutting down http server")
	if err := httpServer.Close(); err != nil {
		log.Warn("failed to close http server", zap.Error(err))
	}
	httpServer = nil
	log.Info("http server is down")
}
