package store_test

import (
	"database/sql"
	"sort"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/showhand-lab/flash-metrics-storage/store"
	"github.com/showhand-lab/flash-metrics-storage/table"
	"github.com/stretchr/testify/require"
)

func TestDefaultMetricsBasic(t *testing.T) {
	db, err := sql.Open("mysql", "root@(127.0.0.1:4000)/test?parseTime=true")
	if err != nil {
		t.Skip("failed to open database", err)
	}
	defer func() {
		require.NoError(t, db.Close())
	}()

	err = db.Ping()
	if err != nil {
		t.Skip("failed to ping database", err)
	}

	for _, stmt := range []string{table.DropMeta, table.DropIndex, table.DropUpdate, table.DropData} {
		_, err = db.Exec(stmt)
		require.NoError(t, err)
	}
	for _, stmt := range []string{table.CreateMeta, table.CreateIndex, table.CreateUpdate, table.CreateData} {
		_, err = db.Exec(stmt)
		require.NoError(t, err)
	}

	now := time.Now().Unix()

	metricStorage := store.NewDefaultMetricStorage(db)
	err = metricStorage.Store(store.TimeSeries{
		Name: "api_http_requests_total",
		Labels: []store.Label{{
			Name:  "method",
			Value: "GET",
		}, {
			Name:  "handler",
			Value: "/messages",
		}},
		Samples: []store.Sample{{
			Timestamp: now,
			Value:     100.0,
		}, {
			Timestamp: now + 15,
			Value:     200.0,
		}},
	})
	require.NoError(t, err)

	err = metricStorage.Store(store.TimeSeries{
		Name: "api_http_requests_total",
		Labels: []store.Label{{
			Name:  "method",
			Value: "POST",
		}, {
			Name:  "handler",
			Value: "/messages",
		}},
		Samples: []store.Sample{{
			Timestamp: now,
			Value:     77.0,
		}},
	})
	require.NoError(t, err)

	ts, err := metricStorage.Query(now, now, "api_http_requests_total", nil)
	require.NoError(t, err)
	sort.Slice(ts[0].Labels, func(i, j int) bool { return ts[0].Labels[i].Name < ts[0].Labels[j].Name })
	sort.Slice(ts[1].Labels, func(i, j int) bool { return ts[1].Labels[i].Name < ts[1].Labels[j].Name })
	require.Equal(t, ts, []store.TimeSeries{{
		Name: "api_http_requests_total",
		Labels: []store.Label{{
			Name:  "handler",
			Value: "/messages",
		}, {
			Name:  "method",
			Value: "GET",
		}},
		Samples: []store.Sample{{
			Timestamp: now,
			Value:     100.0,
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
			Timestamp: now,
			Value:     77.0,
		}},
	}})

	ts, err = metricStorage.Query(now, now+15, "api_http_requests_total", []store.Matcher{{
		LabelName:  "method",
		LabelValue: "GET",
	}})
	require.NoError(t, err)
	sort.Slice(ts[0].Labels, func(i, j int) bool { return ts[0].Labels[i].Name < ts[0].Labels[j].Name })
	require.Equal(t, ts, []store.TimeSeries{{
		Name: "api_http_requests_total",
		Labels: []store.Label{{
			Name:  "handler",
			Value: "/messages",
		}, {
			Name:  "method",
			Value: "GET",
		}},
		Samples: []store.Sample{{
			Timestamp: now,
			Value:     100.0,
		}, {
			Timestamp: now + 15,
			Value:     200.0,
		}},
	}})

	ts, err = metricStorage.Query(now, now+15, "api_http_requests_total", []store.Matcher{{
		LabelName:  "job",
		LabelValue: "tidb",
	}})
	require.NoError(t, err)
	require.Equal(t, len(ts), 0)

	ts, err = metricStorage.Query(now+15, now+15, "api_http_requests_total", []store.Matcher{{
		LabelName:  "method",
		LabelValue: "GET",
		IsNegative: true,
	}})
	require.NoError(t, err)
	require.Equal(t, len(ts), 0)

	ts, err = metricStorage.Query(now, now, "api_http_requests_total", []store.Matcher{{
		LabelName:  "method",
		LabelValue: "%T",
		IsLike:     true,
	}})
	sort.Slice(ts[0].Labels, func(i, j int) bool { return ts[0].Labels[i].Name < ts[0].Labels[j].Name })
	sort.Slice(ts[1].Labels, func(i, j int) bool { return ts[1].Labels[i].Name < ts[1].Labels[j].Name })
	require.Equal(t, ts, []store.TimeSeries{{
		Name: "api_http_requests_total",
		Labels: []store.Label{{
			Name:  "handler",
			Value: "/messages",
		}, {
			Name:  "method",
			Value: "GET",
		}},
		Samples: []store.Sample{{
			Timestamp: now,
			Value:     100.0,
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
			Timestamp: now,
			Value:     77.0,
		}},
	}})

	ts, err = metricStorage.Query(now, now, "api_http_requests_total", []store.Matcher{{
		LabelName:  "method",
		LabelValue: "PO%",
		IsLike:     true,
		IsNegative: true,
	}})
	sort.Slice(ts[0].Labels, func(i, j int) bool { return ts[0].Labels[i].Name < ts[0].Labels[j].Name })
	require.Equal(t, ts, []store.TimeSeries{{
		Name: "api_http_requests_total",
		Labels: []store.Label{{
			Name:  "handler",
			Value: "/messages",
		}, {
			Name:  "method",
			Value: "GET",
		}},
		Samples: []store.Sample{{
			Timestamp: now,
			Value:     100.0,
		}},
	}})
}
