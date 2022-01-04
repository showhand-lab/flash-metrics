package http

import (
	"net"
	"net/http"

	"github.com/pingcap/log"
	"github.com/showhand-lab/flash-metrics-storage/remote"
	"go.uber.org/zap"
)

var (
	httpServer *http.Server = nil
)

func ServeHTTP(listener net.Listener) {
	mux := http.NewServeMux()
	mux.HandleFunc("/write", remote.WriteHandler)
	mux.HandleFunc("/read", remote.ReadHandler)

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
	_ = httpServer.Close()
	log.Info("http server is down")
}
