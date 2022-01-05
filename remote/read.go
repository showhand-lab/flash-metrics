package remote

import (
	"io"
	"io/ioutil"
	"net/http"

	"github.com/golang/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/pingcap/log"
	"github.com/prometheus/prometheus/prompb"
	"github.com/showhand-lab/flash-metrics-storage/store"
	"go.uber.org/zap"
)

func ReadHandler(mstore store.MetricStorage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req, err := decodeReadRequest(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var res []*prompb.TimeSeries
	OUTER:
		for _, query := range req.Queries {
			var metricName string
			var matcher []store.Matcher

			for _, qMatcher := range query.Matchers {
				if qMatcher.Name == "__name__" {
					if qMatcher.Type == prompb.LabelMatcher_EQ {
						metricName = qMatcher.Name
					} else {
						log.Warn("not support other matchers for metric name except equal", zap.Any("query", query))
						continue OUTER
					}
				} else {
					matcher = append(matcher, store.Matcher{
						LabelName:  qMatcher.Name,
						LabelValue: qMatcher.Value,
						IsRE:       qMatcher.Type == prompb.LabelMatcher_NRE || qMatcher.Type == prompb.LabelMatcher_RE,
						IsNegative: qMatcher.Type == prompb.LabelMatcher_NEQ || qMatcher.Type == prompb.LabelMatcher_NRE,
					})
				}
			}

			if metricName == "" {
				log.Warn("metric name not found, ignored", zap.Any("query", query))
				continue
			}

			ts, err := mstore.Query(query.StartTimestampMs, query.EndTimestampMs, metricName, matcher)
			if err != nil {
				log.Warn("failed to query", zap.Any("query", query), zap.Error(err))
				continue
			}

			for _, series := range ts {
				var pLabels []*prompb.Label
				var pSamples []prompb.Sample

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

				res = append(res, &prompb.TimeSeries{
					Labels:  pLabels,
					Samples: pSamples,
				})
			}
		}

		data, err := proto.Marshal(&prompb.ReadResponse{Results: []*prompb.QueryResult{{Timeseries: res}}})
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

	var req prompb.ReadRequest
	if err = proto.Unmarshal(reqBuf, &req); err != nil {
		return nil, err
	}

	return &req, nil
}
