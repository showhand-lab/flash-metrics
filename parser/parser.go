package parser

import (
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql"
	"github.com/showhand-lab/flash-metrics-storage/store"
	"go.uber.org/zap"
	"strconv"
	"time"
)



func NewRangeQuery(storage store.MetricStorage, qry string, start, end time.Time, step time.Duration) (sql string) {

	return ""
}

func NewInstantQuery(storage store.MetricStorage, qry string, time time.Time) (sql string, err error) {
	expr, err := promql.ParseExpr(qry)

	if err != nil {
		log.Warn("parse promql failed", zap.Error(err))
		return "", err
	}

	log.Info("", zap.Any("expr",  expr))

	sql, err = buildSQLForInstantQuery(expr, time)
	if err != nil {
		log.Warn("build sql failed", zap.Error(err))
		return sql, err
	}

	log.Info("build success", zap.String("sql", sql))

	return sql, err
}

func buildSQLForInstantQuery(expr promql.Expr, time time.Time) (sql string, err error) {
	switch x := expr.(type) {
	case *promql.VectorSelector:
		return buildVectorSelector(x, time)
	case *promql.Call:
		return buildCall(x, time)
	case *promql.AggregateExpr:
		return buildAggregateExpr(x, time)
	}

	return "", errors.Errorf("unkown ast node %T", expr)
}

func buildVectorSelector(vc *promql.VectorSelector, t time.Time) (sql string, err error) {
	//var sb strings.Builder

	sql = "select * from flash_metrics_index where metric_name = " + vc.Name + " and timestamp = " + strconv.FormatInt(t.Unix(), 10)

	return sql, nil
}

func buildCall(call *promql.Call, time time.Time) (sql string, err error) {
	switch call.Func.Name {
	case "rate":
		return buildFunctionRate(call, time)
	case "histogram_quantile":
		return buildFunctionHistogramQuantile(call, time)
	case "irate":
		return buildFunctionIRate(call, time)
	}

	return "", errors.Errorf("unkown function %v", call.Func.Name)
}

func buildAggregateExpr(agg *promql.AggregateExpr, time time.Time) (sql string, err error) {
	switch agg.Op {
	case 42: //promql.itemSum:
		return buildAggregationSum(agg, time)
	//case "histogram_quantile":
	//	return buildFunctionRate(call, time)
	}

	return "", errors.Errorf("unkown agg function %v", agg.Op)

}

func buildAggregationSum(agg *promql.AggregateExpr, time time.Time) (sql string, err error) {
	return
}


func buildFunctionRate(call *promql.Call, time time.Time) (sql string, err error) {
	return
}

func buildFunctionIRate(call *promql.Call, time time.Time) (sql string, err error) {
	return
}


func buildFunctionHistogramQuantile(call *promql.Call, time time.Time) (sql string, err error) {
	return
}

