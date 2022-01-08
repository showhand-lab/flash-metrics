package parser

import (
	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql"
	"github.com/showhand-lab/flash-metrics/store"
	"go.uber.org/zap"
	"strconv"
	"time"
)

func NewRangeQuery(storage store.MetricStorage, qry string, start, end time.Time, step time.Duration) (result promql.Value, err error) {
	log.Info("", zap.Any("qry", qry))

	expr, err := promql.ParseExpr(qry)
	if err != nil {
		log.Warn("parse promql failed", zap.Error(err))
		return nil, err
	}

	if solver := tryMatchQPSPattern(expr); solver != nil {
		log.Debug("QPS Pattern attached!")
		tsids, err := solver.GetTsIDs(storage.(*store.DefaultMetricStorage))
		if err != nil {
			return nil, err
		}

		solver.args = append(solver.args, float64(step/time.Second))
		solver.args = append(solver.args, float64(step/time.Second)) // TODO: fix the bug, see TestDoQuery for more details.
		solver.args = append(solver.args, tsids)
		solver.args = append(solver.args, start.Unix())
		solver.args = append(solver.args, end.Unix())

		if err = solver.ExecuteQuery(storage.(*store.DefaultMetricStorage)); err != nil {
			return nil, err
		}

		return solver.result, nil
	}

	log.Warn("no promql pattern matched!")
	return nil, nil
}

func NewInstantQuery(storage store.MetricStorage, qry string, time time.Time) (sql string, err error) {
	log.Info("", zap.Any("qry", qry))

	expr, err := promql.ParseExpr(qry)

	if err != nil {
		log.Warn("parse promql failed", zap.Error(err))
		return "", err
	}

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
	case *promql.MatrixSelector:
		return buildMatrixSelector(x, time)
	case *promql.Call:
		return buildCall(x, time)
	case *promql.AggregateExpr:
		return buildAggregateExpr(x, time)
	case *promql.BinaryExpr:
		return buildBinaryExpr(x, time)
	case *promql.ParenExpr:
		return buildSQLForInstantQuery(x.Expr, time)
	case *promql.UnaryExpr:
		return buildUnaryExpr(x, time)
	}

	return "", errors.Errorf("unkown ast node %T", expr)
}

func buildVectorSelector(vc *promql.VectorSelector, t time.Time) (sql string, err error) {
	//var sb strings.Builder

	sql = "select * from flash_metrics_index where metric_name = " + vc.Name + " and timestamp = " + strconv.FormatInt(t.Unix(), 10)

	return sql, nil
}

func buildMatrixSelector(vc *promql.MatrixSelector, t time.Time) (sql string, err error) {
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
	case "delta":
		return buildFunctionDelta(call, time)
	case "increase":
		return buildFunctionIncrease(call, time)
	}

	return "", errors.Errorf("unkown function %v", call.Func.Name)
}

func buildAggregateExpr(agg *promql.AggregateExpr, time time.Time) (sql string, err error) {
	switch agg.Op {
	case 40:
		return buildAggregationAgg(agg, time)
	case 41:
		return buildAggregationCount(agg, time)
	case 42: //promql.itemSum:
		return buildAggregationSum(agg, time)
	case 43:
		return buildAggregationMin(agg, time)
	case 44:
		return buildAggregationMax(agg, time)
	case 47:
		return buildAggregationTopK(agg, time)
	}

	return "", errors.Errorf("unkown agg function %v", agg.Op)
}

func buildBinaryExpr(bin *promql.BinaryExpr, time time.Time) (sql string, err error) {
	switch bin.Op {
	case 21:
		return buildBinaryOperatorSUB(bin, time)
	case 22:
		return buildBinaryOperatorADD(bin, time)
	case 23:
		return buildBinaryOperatorMUL(bin, time)
	case 24:
		return buildBinaryOperatorMOD(bin, time)
	case 25:
		return buildBinaryOperatorDIV(bin, time)
	case 26:
		return buildBinaryOperatorLAND(bin, time)
	case 27:
		return buildBinaryOperatorLOR(bin, time)
	case 28:
		return buildBinaryOperatorLUnless(bin, time)
	case 29:
		return buildBinaryOperatorEQL(bin, time)
	case 30:
		return buildBinaryOperatorNEQ(bin, time)
	case 31:
		return buildBinaryOperatorLTE(bin, time)
	case 32:
		return buildBinaryOperatorLSS(bin, time)
	case 33:
		return buildBinaryOperatorGTE(bin, time)
	case 34:
		return buildBinaryOperatorGTR(bin, time)
	}

	return "", errors.Errorf("unkown binary operator %v", bin.Op)
}

func buildUnaryExpr(unary *promql.UnaryExpr, time time.Time) (sql string, err error) {
	switch unary.Op {
	case 21: // neg
		return
	}

	return "", errors.Errorf("unkown unary operator %v", unary.Op)
}
