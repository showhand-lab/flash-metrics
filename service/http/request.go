package http

import (
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/pingcap/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/stats"
	"github.com/showhand-lab/flash-metrics-storage/parser"
	"github.com/showhand-lab/flash-metrics-storage/store"
	"go.uber.org/zap"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type QueryData struct {
	ResultType promql.ValueType  `json:"resultType"`
	Result     promql.Value      `json:"result"`
	Stats      *stats.QueryStats `json:"stats,omitempty"`
}

type Response struct {
	Status    string      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType string      `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
	Warnings  []string    `json:"warnings,omitempty"`
}

func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		return time.Unix(int64(s), int64(ns*float64(time.Second))), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}

func QueryHandler(storage store.MetricStorage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
		data := QueryData{
			ResultType: "scalar",
			Result:     scalar,
		}
		respond(w, data)
	}
}

func QueryRangeHandler(storage store.MetricStorage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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

		start, err := parseTime(values["start"][0])
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		end, err := parseTime(values["end"][0])
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		step, err := parseDuration(values["step"][0])
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}

		result, err := parser.NewRangeQuery(storage, values["query"][0], start, end, step)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}

		if result == nil {
			return
		}

		respond(w, QueryData{
			ResultType: result.Type(),
			Result:     result,
		})
		// db.execute(sql)
	}
}

func DefaultHandler(w http.ResponseWriter, r *http.Request) {
	compressed, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	log.Debug("received default http request:", zap.ByteString("request", compressed))

	values, err := url.ParseQuery(string(compressed))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for key, value := range values {
		log.Debug("", zap.String("key", key), zap.Strings("value", value))
	}

	w.WriteHeader(http.StatusOK)
}

func respond(w http.ResponseWriter, data interface{}) {
	b, err := jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(&Response{
		Status: "success",
		Data:   data,
	})
	log.Debug("", zap.String("json", string(b)))

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
