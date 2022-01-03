package store_test

import (
	"database/sql"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"github.com/zhongzc/flash-metrics-write/store"
	"github.com/zhongzc/flash-metrics-write/table"
)

func TestDefaultMetricsBasic(t *testing.T) {
	db, err := sql.Open("mysql", "root@(127.0.0.1:4000)/test")
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
		}},
	})
	require.NoError(t, err)

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
			Timestamp: now + 15,
			Value:     200.0,
		}},
	})
	require.NoError(t, err)
}
