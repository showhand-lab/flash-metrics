package remote_test

import (
	"bytes"
	"context"
	"database/sql"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/showhand-lab/flash-metrics/remote"
	"github.com/showhand-lab/flash-metrics/store"
	"github.com/showhand-lab/flash-metrics/store/model"
	"github.com/showhand-lab/flash-metrics/utils"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/suite"
)

func TestRemoteRead(t *testing.T) {
	if err := utils.PingTiDB(); err != nil {
		t.Skip("failed to ping database", err)
	}
	suite.Run(t, &testRemoteReadSuite{})
}

type testRemoteReadSuite struct {
	suite.Suite
	db      *sql.DB
	storage store.MetricStorage
}

func (s *testRemoteReadSuite) SetupSuite() {
	db, err := utils.SetupDB("test_remote_read")
	s.NoError(err)
	s.db = db
	s.storage = store.NewDefaultMetricStorage(db)
}

func (s *testRemoteReadSuite) TearDownSuite() {
	s.storage.Close()
	s.NoError(utils.TearDownDB("test_remote_read", s.db))
}

func (s *testRemoteReadSuite) TestBasic() {
	now := time.Now().UnixNano() / int64(time.Millisecond)

	err := s.storage.Store(context.Background(), model.TimeSeries{
		Name: "api_http_requests_total",
		Labels: []model.Label{{
			Name:  "method",
			Value: "GET",
		}, {
			Name:  "handler",
			Value: "/messages",
		}},
		Samples: []model.Sample{{
			TimestampMs: now,
			Value:       100.0,
		}, {
			TimestampMs: now + 15,
			Value:       200.0,
		}},
	})
	s.NoError(err)

	err = s.storage.Store(context.Background(), model.TimeSeries{
		Name: "api_http_requests_total",
		Labels: []model.Label{{
			Name:  "method",
			Value: "POST",
		}, {
			Name:  "handler",
			Value: "/messages",
		}},
		Samples: []model.Sample{{
			TimestampMs: now,
			Value:       77.0,
		}},
	})
	s.NoError(err)

	req := &prompb.ReadRequest{
		Queries: []*prompb.Query{{
			StartTimestampMs: now,
			EndTimestampMs:   now,
			Matchers: []*prompb.LabelMatcher{{
				Type:  prompb.LabelMatcher_EQ,
				Name:  "__name__",
				Value: "api_http_requests_total",
			}},
		}, {
			StartTimestampMs: now,
			EndTimestampMs:   now + 15,
			Matchers: []*prompb.LabelMatcher{{
				Type:  prompb.LabelMatcher_EQ,
				Name:  "__name__",
				Value: "api_http_requests_total",
			}, {
				Type:  prompb.LabelMatcher_EQ,
				Name:  "method",
				Value: "GET",
			}},
		}, {
			StartTimestampMs: now,
			EndTimestampMs:   now + 15,
			Matchers: []*prompb.LabelMatcher{{
				Type:  prompb.LabelMatcher_EQ,
				Name:  "__name__",
				Value: "api_http_requests_total",
			}, {
				Type:  prompb.LabelMatcher_EQ,
				Name:  "job",
				Value: "tidb",
			}},
		}, {
			StartTimestampMs: now + 15,
			EndTimestampMs:   now + 15,
			Matchers: []*prompb.LabelMatcher{{
				Type:  prompb.LabelMatcher_EQ,
				Name:  "__name__",
				Value: "api_http_requests_total",
			}, {
				Type:  prompb.LabelMatcher_NEQ,
				Name:  "method",
				Value: "GET",
			}},
		}, {
			StartTimestampMs: now,
			EndTimestampMs:   now,
			Matchers: []*prompb.LabelMatcher{{
				Type:  prompb.LabelMatcher_EQ,
				Name:  "__name__",
				Value: "api_http_requests_total",
			}, {
				Type:  prompb.LabelMatcher_RE,
				Name:  "method",
				Value: ".*T",
			}},
		}, {
			StartTimestampMs: now,
			EndTimestampMs:   now,
			Matchers: []*prompb.LabelMatcher{{
				Type:  prompb.LabelMatcher_EQ,
				Name:  "__name__",
				Value: "api_http_requests_total",
			}, {
				Type:  prompb.LabelMatcher_NRE,
				Name:  "method",
				Value: "PO.*",
			}},
		}},
	}
	pt, err := req.Marshal()
	s.NoError(err)
	buf := bytes.NewBuffer(snappy.Encode(nil, pt))
	httpReq, err := http.NewRequest("GET", "/read", buf)
	s.NoError(err)

	respBuf := bytes.NewBuffer(nil)
	httpResp := utils.NewRespWriter(respBuf)
	remote.ReadHandler(s.storage)(httpResp, httpReq)

	s.True(httpResp.Code >= 200 && httpResp.Code < 300)
	respBytes, err := snappy.Decode(nil, respBuf.Bytes())
	s.NoError(err)

	readResp := &prompb.ReadResponse{}
	err = readResp.Unmarshal(respBytes)
	s.NoError(err)

	for _, q := range readResp.Results {
		for _, t := range q.Timeseries {
			sort.Slice(t.Labels, func(i, j int) bool { return t.Labels[i].Name < t.Labels[j].Name })
		}
	}
	s.Equal(readResp, &prompb.ReadResponse{Results: []*prompb.QueryResult{{
		Timeseries: []*prompb.TimeSeries{{
			Labels: []*prompb.Label{{
				Name:  "__name__",
				Value: "api_http_requests_total",
			}, {
				Name:  "handler",
				Value: "/messages",
			}, {
				Name:  "method",
				Value: "GET",
			}},
			Samples: []prompb.Sample{{
				Timestamp: now,
				Value:     100.0,
			}},
		}, {
			Labels: []*prompb.Label{{
				Name:  "__name__",
				Value: "api_http_requests_total",
			}, {
				Name:  "handler",
				Value: "/messages",
			}, {
				Name:  "method",
				Value: "POST",
			}},
			Samples: []prompb.Sample{{
				Timestamp: now,
				Value:     77.0,
			}},
		}},
	}, {
		Timeseries: []*prompb.TimeSeries{{
			Labels: []*prompb.Label{{
				Name:  "__name__",
				Value: "api_http_requests_total",
			}, {
				Name:  "handler",
				Value: "/messages",
			}, {
				Name:  "method",
				Value: "GET",
			}},
			Samples: []prompb.Sample{{
				Timestamp: now,
				Value:     100.0,
			}, {
				Timestamp: now + 15,
				Value:     200.0,
			}},
		}},
	}, {
		Timeseries: nil,
	}, {
		Timeseries: nil,
	}, {
		Timeseries: []*prompb.TimeSeries{{
			Labels: []*prompb.Label{{
				Name:  "__name__",
				Value: "api_http_requests_total",
			}, {
				Name:  "handler",
				Value: "/messages",
			}, {
				Name:  "method",
				Value: "GET",
			}},
			Samples: []prompb.Sample{{
				Timestamp: now,
				Value:     100.0,
			}},
		}, {
			Labels: []*prompb.Label{{
				Name:  "__name__",
				Value: "api_http_requests_total",
			}, {
				Name:  "handler",
				Value: "/messages",
			}, {
				Name:  "method",
				Value: "POST",
			}},
			Samples: []prompb.Sample{{
				Timestamp: now,
				Value:     77.0,
			}},
		}},
	}, {
		Timeseries: []*prompb.TimeSeries{{
			Labels: []*prompb.Label{{
				Name:  "__name__",
				Value: "api_http_requests_total",
			}, {
				Name:  "handler",
				Value: "/messages",
			}, {
				Name:  "method",
				Value: "GET",
			}},
			Samples: []prompb.Sample{{
				Timestamp: now,
				Value:     100.0,
			}},
		}},
	}}})
}
