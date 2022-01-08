package remote

import (
	"context"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"time"

	"github.com/showhand-lab/flash-metrics-storage/store"
	"github.com/showhand-lab/flash-metrics-storage/store/model"

	"github.com/golang/snappy"
	"github.com/pingcap/log"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/zap"
)

const (
	defaultWriteTimeout = 1 * time.Minute
)

var (
	timeSeriesSliceP = store.TimeSeriesSlicePool{}
	timeSeriesP      = store.TimeSeriesPool{}
)

func WriteHandler(storage store.MetricStorage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(context.Background(), defaultWriteTimeout)
		defer cancel()

		req, err := decodeWriteRequest(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		now := time.Now()
		defer func() {
			log.Info("write time series done", zap.Int("count", len(req.Timeseries)), zap.Duration("duration", time.Since(now)))
		}()

		storeTSs := timeSeriesSliceP.Get()
		defer func() {
			for _, s := range *storeTSs {
				timeSeriesP.Put(s)
			}
			timeSeriesSliceP.Put(storeTSs)
		}()

		for _, series := range req.Timeseries {
			storeTS := timeSeriesP.Get()
			for _, label := range series.Labels {
				if label.Name == "__name__" {
					storeTS.Name = label.Value
				} else {
					storeTS.Labels = append(storeTS.Labels, model.Label{
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
				storeTS.Samples = append(storeTS.Samples, model.Sample{
					TimestampMs: sample.Timestamp,
					Value:       sample.Value,
				})
			}

			*storeTSs = append(*storeTSs, storeTS)
		}

		if err = storage.BatchStore(ctx, *storeTSs); err != nil {
			log.Warn("failed to store time series", zap.Error(err))
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
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
