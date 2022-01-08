package parser

import (
	"github.com/prometheus/prometheus/promql"
	"time"
)

func buildFunctionRate(call *promql.Call, time time.Time) (sql string, err error) {
	return
}

func buildFunctionIRate(call *promql.Call, time time.Time) (sql string, err error) {
	return
}

func buildFunctionHistogramQuantile(call *promql.Call, time time.Time) (sql string, err error) {
	return
}

func buildFunctionDelta(call *promql.Call, time time.Time) (sql string, err error) {
	return
}

func buildFunctionIncrease(call *promql.Call, time time.Time) (sql string, err error) {
	return
}
