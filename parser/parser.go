package parser

import (
	"github.com/pingcap/log"
	"github.com/prometheus/prometheus/promql"
	"go.uber.org/zap"
	"time"
)

func NewRangeQuery(qry string, start, end time.Time, step time.Duration) (sql string) {
	expr, err := promql.ParseExpr(qry)

	if err != nil {
		log.Warn("parse promql failed", zap.Error(err))
		return ""
	}

	log.Info("", zap.Any("expr",  expr))

	return ""
}

func NewQuery(promql string, time time.Time) (sql string) {
	return ""
}