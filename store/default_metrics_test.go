package store_test

import (
	"context"
	"database/sql"
	"sort"
	"testing"
	"time"

	"github.com/showhand-lab/flash-metrics/store"
	"github.com/showhand-lab/flash-metrics/store/model"
	"github.com/showhand-lab/flash-metrics/utils"

	"github.com/stretchr/testify/suite"

	_ "github.com/go-sql-driver/mysql"
)

func TestDefaultMetrics(t *testing.T) {
	if err := utils.PingTiDB(); err != nil {
		t.Skip("failed to ping database", err)
	}
	suite.Run(t, &testDefaultMetricsSuite{})
}

type testDefaultMetricsSuite struct {
	suite.Suite
	db *sql.DB
}

func (s *testDefaultMetricsSuite) SetupSuite() {
	db, err := utils.SetupDB("test_default_metrics")
	s.NoError(err)
	s.db = db
}

func (s *testDefaultMetricsSuite) TearDownSuite() {
	s.NoError(utils.TearDownDB("test_default_metrics", s.db))
}

func (s *testDefaultMetricsSuite) TestDefaultMetricsBasic() {
	now := time.Now().UnixNano() / int64(time.Millisecond)

	metricStorage := store.NewDefaultMetricStorage(s.db)
	defer metricStorage.Close()

	err := metricStorage.Store(context.Background(), model.TimeSeries{
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

	err = metricStorage.Store(context.Background(), model.TimeSeries{
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

	err = metricStorage.BatchStore(context.Background(), []*model.TimeSeries{{
		Name: "api_http_requests_total",
		Labels: []model.Label{{
			Name:  "scheme",
			Value: "http",
		}},
		Samples: []model.Sample{{
			TimestampMs: now,
			Value:       42.0,
		}},
	}, {
		Name: "api_http_requests_total",
		Labels: []model.Label{{
			Name:  "scheme",
			Value: "https",
		}},
		Samples: []model.Sample{{
			TimestampMs: now,
			Value:       88.0,
		}},
	}})
	s.NoError(err)

	ts, err := metricStorage.Query(context.Background(), now, now, "api_http_requests_total", nil)
	s.NoError(err)
	sort.Slice(ts[0].Labels, func(i, j int) bool { return ts[0].Labels[i].Name < ts[0].Labels[j].Name })
	sort.Slice(ts[1].Labels, func(i, j int) bool { return ts[1].Labels[i].Name < ts[1].Labels[j].Name })
	s.Equal(ts, []model.TimeSeries{{
		Name: "api_http_requests_total",
		Labels: []model.Label{{
			Name:  "handler",
			Value: "/messages",
		}, {
			Name:  "method",
			Value: "GET",
		}},
		Samples: []model.Sample{{
			TimestampMs: now,
			Value:       100.0,
		}},
	}, {
		Name: "api_http_requests_total",
		Labels: []model.Label{{
			Name:  "handler",
			Value: "/messages",
		}, {
			Name:  "method",
			Value: "POST",
		}},
		Samples: []model.Sample{{
			TimestampMs: now,
			Value:       77.0,
		}},
	}, {
		Name: "api_http_requests_total",
		Labels: []model.Label{{
			Name:  "scheme",
			Value: "http",
		}},
		Samples: []model.Sample{{
			TimestampMs: now,
			Value:       42.0,
		}},
	}, {
		Name: "api_http_requests_total",
		Labels: []model.Label{{
			Name:  "scheme",
			Value: "https",
		}},
		Samples: []model.Sample{{
			TimestampMs: now,
			Value:       88.0,
		}},
	}})

	ts, err = metricStorage.Query(context.Background(), now, now+15, "api_http_requests_total", []model.Matcher{{
		LabelName:  "method",
		LabelValue: "GET",
	}})
	s.NoError(err)
	sort.Slice(ts[0].Labels, func(i, j int) bool { return ts[0].Labels[i].Name < ts[0].Labels[j].Name })
	s.Equal(ts, []model.TimeSeries{{
		Name: "api_http_requests_total",
		Labels: []model.Label{{
			Name:  "handler",
			Value: "/messages",
		}, {
			Name:  "method",
			Value: "GET",
		}},
		Samples: []model.Sample{{
			TimestampMs: now,
			Value:       100.0,
		}, {
			TimestampMs: now + 15,
			Value:       200.0,
		}},
	}})

	ts, err = metricStorage.Query(context.Background(), now, now+15, "api_http_requests_total", []model.Matcher{{
		LabelName:  "job",
		LabelValue: "tidb",
	}})
	s.NoError(err)
	s.Equal(len(ts), 0)

	ts, err = metricStorage.Query(context.Background(), now+15, now+15, "api_http_requests_total", []model.Matcher{{
		LabelName:  "method",
		LabelValue: "GET",
		IsNegative: true,
	}})
	s.NoError(err)
	s.Equal(len(ts), 0)

	ts, err = metricStorage.Query(context.Background(), now, now, "api_http_requests_total", []model.Matcher{{
		LabelName:  "method",
		LabelValue: ".*T",
		IsRE:       true,
	}})
	s.NoError(err)
	sort.Slice(ts[0].Labels, func(i, j int) bool { return ts[0].Labels[i].Name < ts[0].Labels[j].Name })
	sort.Slice(ts[1].Labels, func(i, j int) bool { return ts[1].Labels[i].Name < ts[1].Labels[j].Name })
	s.Equal(ts, []model.TimeSeries{{
		Name: "api_http_requests_total",
		Labels: []model.Label{{
			Name:  "handler",
			Value: "/messages",
		}, {
			Name:  "method",
			Value: "GET",
		}},
		Samples: []model.Sample{{
			TimestampMs: now,
			Value:       100.0,
		}},
	}, {
		Name: "api_http_requests_total",
		Labels: []model.Label{{
			Name:  "handler",
			Value: "/messages",
		}, {
			Name:  "method",
			Value: "POST",
		}},
		Samples: []model.Sample{{
			TimestampMs: now,
			Value:       77.0,
		}},
	}})

	ts, err = metricStorage.Query(context.Background(), now, now, "api_http_requests_total", []model.Matcher{{
		LabelName:  "method",
		LabelValue: "PO.*",
		IsRE:       true,
		IsNegative: true,
	}})
	s.NoError(err)
	sort.Slice(ts[0].Labels, func(i, j int) bool { return ts[0].Labels[i].Name < ts[0].Labels[j].Name })
	s.Equal(ts, []model.TimeSeries{{
		Name: "api_http_requests_total",
		Labels: []model.Label{{
			Name:  "handler",
			Value: "/messages",
		}, {
			Name:  "method",
			Value: "GET",
		}},
		Samples: []model.Sample{{
			TimestampMs: now,
			Value:       100.0,
		}},
	}, {
		Name: "api_http_requests_total",
		Labels: []model.Label{{
			Name:  "scheme",
			Value: "http",
		}},
		Samples: []model.Sample{{
			TimestampMs: now,
			Value:       42.0,
		}},
	}, {
		Name: "api_http_requests_total",
		Labels: []model.Label{{
			Name:  "scheme",
			Value: "https",
		}},
		Samples: []model.Sample{{
			TimestampMs: now,
			Value:       88.0,
		}},
	}})
}
