package parser

import (
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/showhand-lab/flash-metrics-storage/metas"
	"github.com/showhand-lab/flash-metrics-storage/store"
	"strconv"
	"strings"
)

var qpsPattern =
`
select tsid, ts-ts%? tsmod, (max(v)-min(v))/? rate_v
from flash_metrics_data
where flash_metrics_data.tsid in (?)
and ts >= ? and ts < ?
group by tsid, tsmod
`

type QPSSolver struct {
	sumByName []string
	metricsName string
	LabelMatchers []*labels.Matcher

	// step, step, tsids, start, end
	args []interface{}
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

	return &QPSSolver {
		sumByName: agg.Grouping,
		metricsName: matrix.Name,
		LabelMatchers: matrix.LabelMatchers,
	}
}

func (solver *QPSSolver) GetTsIDs(storage *store.DefaultMetricStorage) (tsids_string string, err error) {
	m, err := storage.QueryMeta(solver.metricsName)
	if err != nil {
		return
	}

	for _, matcher := range solver.LabelMatchers {
		if _, ok := m.Labels[metas.LabelName(matcher.Name)]; !ok {
			return "", errors.Errorf("label not found!")
		}
	}

	// todo：use interfaceSliceP
	var args []interface{}
	var sb strings.Builder

	sb.WriteString(
`
select _tidb_rowid
from flash_metrics_index
where metric_name = ?
`)
	args = append(args, solver.metricsName)

	for _, matcher := range solver.LabelMatchers {
		labelID := m.Labels[metas.LabelName(matcher.Name)]
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

	rows, err := storage.DB.Query(sb.String(), args)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var tsids []string
	for rows.Next() {
		var x []interface{}
		if err = rows.Scan(x...); err != nil {
			return "", err
		}
		tsids = append(tsids, x[0].(string))
	}

	return strings.Join(tsids, ","), nil
}