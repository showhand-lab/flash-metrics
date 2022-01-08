package remote

import (
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"time"

	"github.com/showhand-lab/flash-metrics/store"

	"github.com/golang/snappy"
	"github.com/pingcap/log"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/zap"
)

func WriteHandler(storage store.MetricStorage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, err := decodeWriteRequest(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		now := time.Now()
		defer func() {
			log.Info("write timeseries done", zap.Int("count", len(req.Timeseries)), zap.Duration("duration", time.Since(now)))
		}()

		for _, series := range req.Timeseries {
			var storeTS store.TimeSeries
			for _, label := range series.Labels {
				if label.Name == "__name__" {
					storeTS.Name = label.Value
				} else {
					storeTS.Labels = append(storeTS.Labels, store.Label{
						Name:  label.Name,
						Value: label.Value,
					})
				}
			}
			if storeTS.Name == "" {
				log.Warn("metric name not found, ignored", zap.Any("timeseries", series))
				continue
			}
			for _, sample := range series.Samples {
				if math.IsNaN(sample.Value) {
					continue
				}
				storeTS.Samples = append(storeTS.Samples, store.Sample{
					TimestampMs: sample.Timestamp,
					Value:       sample.Value,
				})
			}

			n := time.Now()
			if err = storage.Store(storeTS); err != nil {
				log.Warn("failed to store timeseries", zap.Error(err), zap.Any("timeseries", series))
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			log.Debug("write single timeseries", zap.Duration("duration", time.Since(n)))
		}

		_, _ = w.Write([]byte("ok"))
	}
}

func decodeWriteRequest(r io.Reader) (*prompb.WriteRequest, error) {
	compressed, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, err
	}

	req := &prompb.WriteRequest{}
	if err = req.Unmarshal(reqBuf); err != nil {
		return nil, err
	}

	return req, nil
}
