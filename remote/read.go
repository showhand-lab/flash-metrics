package remote

import (
	"context"
	"io"
	"io/ioutil"
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
	defaultReadTimeout = 1 * time.Minute
)

var (
	queryResultP = QueryResultSlicePool{}
)

func ReadHandler(storage store.MetricStorage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(context.Background(), defaultReadTimeout)
		defer cancel()

		req, err := decodeReadRequest(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		queryResults := queryResultP.Get()
		defer queryResultP.Put(queryResults)

	OUTER:
		for _, query := range req.Queries {
			var metricName string
			var matcher []model.Matcher

			for _, qMatcher := range query.Matchers {
				if qMatcher.Name == "__name__" {
					if qMatcher.Type == prompb.LabelMatcher_EQ {
						metricName = qMatcher.Value
					} else {
						log.Warn("not support other matchers for metric name except equal", zap.Any("query", query))
						continue OUTER
					}
				} else {
					matcher = append(matcher, model.Matcher{
						LabelName:  qMatcher.Name,
						LabelValue: qMatcher.Value,
						IsRE:       qMatcher.Type == prompb.LabelMatcher_NRE || qMatcher.Type == prompb.LabelMatcher_RE,
						IsNegative: qMatcher.Type == prompb.LabelMatcher_NEQ || qMatcher.Type == prompb.LabelMatcher_NRE,
					})
				}
			}

			if metricName == "" {
				log.Warn("metric name not found, ignored", zap.Any("query", query))
				*queryResults = append(*queryResults, nil)
				continue
			}

			ts, err := storage.Query(ctx, query.StartTimestampMs, query.EndTimestampMs, metricName, matcher)
			if err != nil {
				log.Warn("failed to query", zap.Any("query", query), zap.Error(err))
				*queryResults = append(*queryResults, nil)
				continue
			}

			seriesRes := make([]*prompb.TimeSeries, 0, len(ts))
			for _, series := range ts {
				pLabels := make([]*prompb.Label, 0, len(series.Labels)+1)
				pSamples := make([]prompb.Sample, 0, len(series.Samples))

				pLabels = append(pLabels, &prompb.Label{
					Name:  "__name__",
					Value: series.Name,
				})

				for _, l := range series.Labels {
					pLabels = append(pLabels, &prompb.Label{
						Name:  l.Name,
						Value: l.Value,
					})
				}

				for _, s := range series.Samples {
					pSamples = append(pSamples, prompb.Sample{
						Timestamp: s.TimestampMs,
						Value:     s.Value,
					})
				}

				seriesRes = append(seriesRes, &prompb.TimeSeries{
					Labels:  pLabels,
					Samples: pSamples,
				})
			}
			*queryResults = append(*queryResults, &prompb.QueryResult{Timeseries: seriesRes})
		}

		resp := &prompb.ReadResponse{Results: *queryResults}
		data, err := resp.Marshal()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.Header().Set("Content-Encoding", "snappy")
		compressed := snappy.Encode(nil, data)
		_, _ = w.Write(compressed)
	}
}

func decodeReadRequest(r io.Reader) (*prompb.ReadRequest, error) {
	compressed, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, err
	}

	req := &prompb.ReadRequest{}
	if err = req.Unmarshal(reqBuf); err != nil {
		return nil, err
	}

	return req, nil
}
