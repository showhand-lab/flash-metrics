package parser

import (
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func TestQueryParse(t *testing.T) {
	NewRangeQuery("histogram_quantile(0.99, rate(binlog_pump_rpc_duration_seconds_bucket{method=\"WriteBinlog\"}[1m]))", time.Now(), time.Now(), time.Second)
}