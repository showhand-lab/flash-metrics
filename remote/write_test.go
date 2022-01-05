package remote_test

import (
	"bytes"
	"database/sql"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/showhand-lab/flash-metrics-storage/remote"
	"github.com/showhand-lab/flash-metrics-storage/store"
	"github.com/showhand-lab/flash-metrics-storage/utils"
	"github.com/stretchr/testify/suite"
)

func TestRemoteWrite(t *testing.T) {
	if err := utils.PingTiDB(); err != nil {
		t.Skip("failed to ping database", err)
	}
	suite.Run(t, &testRemoteWriteSuite{})
}

type testRemoteWriteSuite struct {
	suite.Suite
	db     *sql.DB
	mstore store.MetricStorage
}

func (s *testRemoteWriteSuite) SetupSuite() {
	db, err := utils.SetupDB("test_remote_write")
	s.NoError(err)
	s.db = db
	s.mstore = store.NewDefaultMetricStorage(db)
}

func (s *testRemoteWriteSuite) TearDownSuite() {
	s.NoError(utils.TearDownDB("test_remote_write", s.db))
}

func (s *testRemoteWriteSuite) TestBasic() {
	now := time.Now().UnixNano() / int64(time.Millisecond)

	req := &prompb.WriteRequest{
		Timeseries: []*prompb.TimeSeries{{
			Labels: []*prompb.Label{{
				Name:  "__name__",
				Value: "api_http_requests_total",
			}, {
				Name:  "method",
				Value: "GET",
			}, {
				Name:  "handler",
				Value: "/messages",
			}},
			Samples: []prompb.Sample{{
				Timestamp: now,
				Value:     100.0,
			}, {
				Timestamp: now + 15,
				Value:     200.0,
			}},
		}, {
			Labels: []*prompb.Label{{
				Name:  "__name__",
				Value: "api_http_requests_total",
			}, {
				Name:  "method",
				Value: "POST",
			}, {
				Name:  "handler",
				Value: "/messages",
			}},
			Samples: []prompb.Sample{{
				Timestamp: now,
				Value:     77.0,
			}},
		}},
	}
	pt, err := req.Marshal()
	s.NoError(err)

	buf := bytes.NewBuffer(snappy.Encode(nil, pt))
	httpReq, err := http.NewRequest("POST", "/write", buf)
	s.NoError(err)

	respBuf := bytes.NewBuffer(nil)
	httpResp := utils.NewRespWriter(respBuf)
	remote.WriteHandler(s.mstore)(httpResp, httpReq)

	s.True(httpResp.Code >= 200 && httpResp.Code < 300)
	s.Equal(respBuf.String(), "ok")

	ts, err := s.mstore.Query(now, now+15, "api_http_requests_total", nil)
	s.NoError(err)
	sort.Slice(ts[0].Labels, func(i, j int) bool { return ts[0].Labels[i].Name < ts[0].Labels[j].Name })
	sort.Slice(ts[1].Labels, func(i, j int) bool { return ts[1].Labels[i].Name < ts[1].Labels[j].Name })
	s.Equal(ts, []store.TimeSeries{{
		Name: "api_http_requests_total",
		Labels: []store.Label{{
			Name:  "handler",
			Value: "/messages",
		}, {
			Name:  "method",
			Value: "GET",
		}},
		Samples: []store.Sample{{
			TimestampMs: now,
			Value:       100.0,
		}, {
			TimestampMs: now + 15,
			Value:       200.0,
		}},
	}, {
		Name: "api_http_requests_total",
		Labels: []store.Label{{
			Name:  "handler",
			Value: "/messages",
		}, {
			Name:  "method",
			Value: "POST",
		}},
		Samples: []store.Sample{{
			TimestampMs: now,
			Value:       77.0,
		}},
	}})
}
