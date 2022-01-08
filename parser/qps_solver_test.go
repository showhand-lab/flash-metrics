package parser

import (
	"context"
	"testing"
	"time"

	"github.com/showhand-lab/flash-metrics-storage/store"
	"github.com/showhand-lab/flash-metrics-storage/store/model"
	"github.com/showhand-lab/flash-metrics-storage/utils"

	"github.com/stretchr/testify/require"
)

func TestQPSSolver(t *testing.T) {
	db, err := utils.SetupDB("test_qps_sovler")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, utils.TearDownDB("test_qps_sovler", db))
	}()

	metricStorage := store.NewDefaultMetricStorage(db)
	defer metricStorage.Close()

	now := time.Now()
	err = metricStorage.Store(context.Background(), model.TimeSeries{
		Name: "tiflash_coprocessor_request_count",
		Labels: []model.Label{{
			Name:  "tidb_cluster",
			Value: "",
		}, {
			Name:  "instance",
			Value: "()",
		}, {
			Name:  "type",
			Value: "cop",
		}},
		Samples: []model.Sample{{
			TimestampMs: now.UnixNano() / int64(time.Millisecond),
			Value:       10,
		}},
	})
	require.NoError(t, err)

	_, err = NewRangeQuery(metricStorage, "sum(rate(tiflash_coprocessor_request_count{tidb_cluster=\"\", instance=\"()\"}[1m])) by (type)",
		now, now.Add(1*time.Second), 15*time.Second)
	require.NoError(t, err)
}

func TestDoQuery(t *testing.T) {
	db, err := utils.SetupDB("test_do_query")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, utils.TearDownDB("test_do_query", db))
	}()

	metricStorage := store.NewDefaultMetricStorage(db)
	defer metricStorage.Close()

	solver := &QPSSolver{
		groupByNames:  []string{"type"},
		metricName:    "tiflash_coprocessor_request_count",
		labelMatchers: nil,
		args:          []interface{}{15.0, 15.0, "101,105,99,107,103,104,100,98,102,106", 1641586050, 1641589650},
	}

	_, err = db.Query("select tsid, ts-ts%15 tsmod, (max(v)-min(v))/15 rate_v from flash_metrics_data where flash_metrics_data.tsid in (101,105,99,107,103,104,100,98,102,106) and ts >= 1641586050 and ts <= 1641589650 group by tsid, tsmod")
	require.NoError(t, err)

	//db.Exec("use test")
	//db.Exec("set tidb_enforce_mpp=1")
	//db.Exec(" set tidb_partition_prune_mode=dynamic;")
	//// will trigger a bug "Wrong precision:18446744073709551615", confusing.
	//rows, err := metricStorage.DB.Query("select ts % ? x from flash_metrics_data where tsid=1 group by x", 15)
	//if err != nil {
	//	log.Error("",zap.Error(err))
	//	t.Fail()
	//} else {
	//	for rows.Next() {
	//		row := make([]string, 5, 5)
	//		if err = rows.Scan(&row[0], &row[1], &row[2], &row[3], &row[4]); err != nil {
	//			log.Error("",zap.Error(err))
	//			t.Fail()
	//		}
	//		log.Info("",zap.Strings("row", row))
	//	}
	//}

	err = solver.ExecuteQuery(metricStorage)
	require.NoError(t, err)
}
