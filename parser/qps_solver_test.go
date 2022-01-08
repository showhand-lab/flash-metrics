package parser

import (
	"github.com/pingcap/log"
	"github.com/showhand-lab/flash-metrics/store"
	"github.com/showhand-lab/flash-metrics/utils"
	"go.uber.org/zap"
	"testing"
	"time"
)

func TestQPSSolver(t *testing.T) {
	db, err := utils.SetupDB("test")
	if err != nil {
		log.Fatal("", zap.Error(err))
	}

	metricStorage := store.NewDefaultMetricStorage(db)

	loc, _ := time.LoadLocation("Local")
	start, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-08 04:07:30 +0800", loc)
	end, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-08 05:07:30 +0800", loc)

	if _, err = NewRangeQuery(metricStorage, "sum(rate(tiflash_coprocessor_request_count{tidb_cluster=\"\", instance=~\"()\"}[1m])) by (type)",
		start, end, 15000000000); err != nil {
		log.Error("", zap.Error(err))
		t.Fail()
	}

}

func TestDoQuery(t *testing.T) {
	db, err := utils.SetupDB("test")
	if err != nil {
		log.Fatal("", zap.Error(err))
	}
	metricStorage := store.NewDefaultMetricStorage(db)

	solver := &QPSSolver{
		groupByNames:  []string{"type"},
		metricName:    "tiflash_coprocessor_request_count",
		labelMatchers: nil,
		args:          []interface{}{15.0, 15.0, "101,105,99,107,103,104,100,98,102,106", 1641586050, 1641589650},
	}

	_, err = db.Query("select tsid, ts-ts%15 tsmod, (max(v)-min(v))/15 rate_v from flash_metrics_data where flash_metrics_data.tsid in (101,105,99,107,103,104,100,98,102,106) and ts >= 1641586050 and ts <= 1641589650 group by tsid, tsmod")
	if err != nil {
		log.Error("", zap.Error(err))
		t.Fail()
	}

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

	db.Exec("use test")
	if err = solver.ExecuteQuery(metricStorage); err != nil {
		log.Error("", zap.Error(err))
		t.Fail()
	}

}
