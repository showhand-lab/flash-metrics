package parser

import (
	"time"

	"github.com/prometheus/prometheus/promql"
)

func buildAggregationAgg(agg *promql.AggregateExpr, time time.Time) (sql string, err error) {
	return
}

func buildAggregationSum(agg *promql.AggregateExpr, time time.Time) (sql string, err error) {
	return
}

func buildAggregationCount(agg *promql.AggregateExpr, time time.Time) (sql string, err error) {
	return
}

func buildAggregationMax(agg *promql.AggregateExpr, time time.Time) (sql string, err error) {
	return
}

func buildAggregationMin(agg *promql.AggregateExpr, time time.Time) (sql string, err error) {
	return
}

func buildAggregationTopK(agg *promql.AggregateExpr, time time.Time) (sql string, err error) {
	return
}
