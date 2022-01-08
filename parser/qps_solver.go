package parser

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"github.com/showhand-lab/flash-metrics/metas"
	"github.com/showhand-lab/flash-metrics/store"

	"github.com/pingcap/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"go.uber.org/zap"
)

const qpsPattern = `
select tsid, (unix_timestamp(ts)-unix_timestamp(ts)mod %v)*1000 tsmod, (max(v)-min(v))/%v
from flash_metrics_data
where tsid in (%v)
and unix_timestamp(ts) >= %v and unix_timestamp(ts) <= %v
group by tsid, tsmod
order by tsmod
`

type QPSSolver struct {
	groupByNames  []string
	metricName    string
	labelMatchers []*labels.Matcher

	// step, step, tsids, start, end
	args []interface{}

	result            promql.Matrix
	matrixIndexHelper map[int]int // key means tsid, value means index of result.
}

func tryMatchQPSPattern(expr promql.Expr) *QPSSolver {
	agg, suc := expr.(*promql.AggregateExpr)
	if !suc || agg.Op != 42 {
		return nil
	}
	rate, suc := agg.Expr.(*promql.Call)
	if !suc || rate.Func.Name != "rate" {
		return nil
	}
	matrix, suc := rate.Args[0].(*promql.MatrixSelector)
	if !suc {
		return nil
	}

	return &QPSSolver{
		groupByNames:  agg.Grouping,
		metricName:    matrix.Name,
		labelMatchers: matrix.LabelMatchers,
	}
}

func (solver *QPSSolver) GetTsIDs(storage *store.DefaultMetricStorage) (tsids_string string, err error) {
	m, err := storage.QueryMeta(context.Background(), solver.metricName)
	if err != nil {
		return
	}

	// todo：use interfaceSliceP
	var args []interface{}
	var sb strings.Builder

	sb.WriteString(
		`
select _tidb_rowid
`)
	groupByCount := 0
	for _, groupByName := range solver.groupByNames {
		labelID, ok := m.Labels[metas.LabelName(groupByName)]
		if !ok {
			log.Error("group by label not found!", zap.String("label", groupByName))
			return "", errors.Errorf("group by label not found!")
		}
		groupByCount++
		sb.WriteString(", label")
		sb.WriteString(strconv.Itoa(int(labelID)))
	}

	sb.WriteString(`
from flash_metrics_index
where metric_name = ?
`)
	args = append(args, solver.metricName)

	for _, matcher := range solver.labelMatchers {
		if matcher.Name == "__name__" {
			continue
		}

		labelID, ok := m.Labels[metas.LabelName(matcher.Name)]
		if !ok {
			if (matcher.Type == labels.MatchEqual || matcher.Type == labels.MatchRegexp) && matcher.Value == "" {
				continue
			}
			log.Error("label not found!", zap.String("label", matcher.Name))
			continue
		}
		sb.WriteString("AND label")
		sb.WriteString(strconv.Itoa(int(labelID)))

		switch matcher.Type {
		case labels.MatchEqual:
			sb.WriteString(" = ?\n")
		case labels.MatchNotEqual:
			sb.WriteString(" != ?\n")
		case labels.MatchRegexp:
			sb.WriteString(" REGEXP ?\n")
		case labels.MatchNotRegexp:
			sb.WriteString(" NOT REGEXP ?\n")
		}
		args = append(args, matcher.Value)
	}

	rows, err := storage.DB.Query(sb.String(), args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	solver.matrixIndexHelper = make(map[int]int)

	var tsids []string
	for rows.Next() {
		row := make([]interface{}, groupByCount+1)
		for index := range row {
			if index == 0 {
				var i int
				row[index] = &i
			} else {
				var s string
				row[index] = &s
			}
		}
		if err = rows.Scan(row...); err != nil {
			return "", err
		}

		tsid := *row[0].(*int)
		tsids = append(tsids, strconv.Itoa(tsid))

		var lbs labels.Labels
		for index, str := range row[1:] {
			lbs = append(lbs, labels.Label{
				Name:  solver.groupByNames[index],
				Value: *str.(*string)})
		}
		solver.updateResultLabel(tsid, lbs)
	}

	return strings.Join(tsids, ","), nil
}

func (solver *QPSSolver) updateResultLabel(tsid int, lbs labels.Labels) {
	// naive for loop, because the labels count won't be large.
	index := 0
	for ; index < len(solver.result); index++ {
		if reflect.DeepEqual(solver.result[index].Metric, lbs) {
			break
		}
	}
	if index >= len(solver.result) {
		solver.result = append(solver.result, promql.Series{Metric: lbs})
	}
	solver.matrixIndexHelper[tsid] = index
}

func (solver *QPSSolver) ExecuteQuery(storage *store.DefaultMetricStorage) (err error) {
	sql := fmt.Sprintf(qpsPattern, solver.args...)
	rows, err := storage.DB.Query(sql)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var tsid int
		var tsmod float64
		var value float64
		if err = rows.Scan(&tsid, &tsmod, &value); err != nil {
			log.Warn("", zap.Error(err))
			return err
		}

		// assert order by tsid.
		series := &solver.result[solver.matrixIndexHelper[tsid]]
		if series.Points != nil && series.Points[len(series.Points)-1].T == int64(tsmod) {
			series.Points[len(series.Points)-1].V += value
		} else {
			series.Points = append(series.Points, promql.Point{T: int64(tsmod), V: value})
		}
	}

	// mock data
	//i := 0
	//	for now := solver.args[3].(int64); now <= solver.args[3].(int64) + int64(solver.args[0].(float64)) * 10; now += int64(solver.args[0].(float64)) {
	//		solver.result[i].Points = append(solver.result[i].Points, promql.Point{T: now*1000, V: 15})
	//	}

	return nil
}
