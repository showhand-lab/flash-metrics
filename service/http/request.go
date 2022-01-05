package http

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/pingcap/log"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/stats"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"
	"net/url"
)

type queryData struct {
	ResultType promql.ValueType  `json:"resultType"`
	Result     promql.Value      `json:"result"`
	Stats      *stats.QueryStats `json:"stats,omitempty"`
}

type response struct {
	Status    string      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType string      `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
	Warnings  []string    `json:"warnings,omitempty"`
}

func QueryHandler(w http.ResponseWriter, r *http.Request) {
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Debug("received http request:", zap.ByteString("request", compressed))

	values, err := url.ParseQuery(string(compressed))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for key, value := range values {
		log.Debug("", zap.String("key", key), zap.Strings("value", value))
	}

	scalar := promql.Scalar{T: 1, V: 1.0}
	data := queryData{
		"scalar",
		scalar,
		nil,
	}
	respond(w, data)
}

func respond(w http.ResponseWriter, data interface{}) {
	b, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(&response{
		Status: "success",
		Data:   data,
	})

	if err != nil {
		log.Warn("error marshaling json response", zap.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if n, err := w.Write(b); err != nil {
		log.Warn("error writing response", zap.Int("bytesWritten", n), zap.Error(err))
	}
}
