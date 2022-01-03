package store

import (
	"database/sql"
	"strconv"
	"strings"
	"time"

	"github.com/zhongzc/flash-metrics-write/metas"
)

var (
	argSliceP = ArgSlicePool{}
)

type DefaultMetricStorage struct {
	*metas.DefaultMetaStorage

	db *sql.DB
}

func NewDefaultMetricStorage(db *sql.DB) *DefaultMetricStorage {
	return &DefaultMetricStorage{DefaultMetaStorage: metas.NewDefaultMetaStorage(db), db: db}
}

var _ MetricStorage = &DefaultMetricStorage{}

func (d *DefaultMetricStorage) Store(timeSeries TimeSeries) error {
	if len(timeSeries.Samples) == 0 {
		return nil
	}

	labelName := make([]string, 0, len(timeSeries.Labels))
	for _, l := range timeSeries.Labels {
		labelName = append(labelName, l.Name)
	}

	m, err := d.StoreMeta(timeSeries.Name, labelName)
	if err != nil {
		return err
	}

	// insert index
	if err = d.insertIndex(timeSeries, m); err != nil {
		return err
	}

	// get tsid
	tsid, err := d.getTSID(timeSeries, m)
	if err != nil {
		return err
	}

	// insert updated date
	if err = d.insertUpdatedDate(tsid, timeSeries); err != nil {
		return err
	}

	// insert data
	return d.insertData(tsid, timeSeries)
}

func (d *DefaultMetricStorage) Query(start, end int64, metricsName string, matchers []Matcher) (*TimeSeries, error) {
	panic("implement me")
}

func (d *DefaultMetricStorage) insertIndex(timeSeries TimeSeries, m *metas.Meta) error {
	args := argSliceP.Get()
	defer argSliceP.Put(args)
	var sb strings.Builder

	sb.WriteString("INSERT IGNORE INTO flash_metrics_index (metric_name")
	*args = append(*args, timeSeries.Name)
	for _, label := range timeSeries.Labels {
		labelID := m.Labels[metas.LabelName(label.Name)]
		sb.WriteString(", label")
		sb.WriteString(strconv.Itoa(int(labelID)))
		*args = append(*args, label.Value)
	}
	sb.WriteString(") VALUES (?")
	for range timeSeries.Labels {
		sb.WriteString(", ?")
	}
	sb.WriteString(");")
	_, err := d.db.Exec(sb.String(), *args...)
	return err
}

func (d *DefaultMetricStorage) getTSID(timeSeries TimeSeries, m *metas.Meta) (int64, error) {
	args := argSliceP.Get()
	defer argSliceP.Put(args)
	var sb strings.Builder

	sb.WriteString("SELECT _tidb_rowid FROM flash_metrics_index WHERE metric_name = ?")
	*args = append(*args, timeSeries.Name)
	for _, label := range timeSeries.Labels {
		labelID := m.Labels[metas.LabelName(label.Name)]
		sb.WriteString("AND label")
		sb.WriteString(strconv.Itoa(int(labelID)))
		sb.WriteString(" = ? ")
		*args = append(*args, label.Value)
	}
	sb.WriteByte(';')
	row := d.db.QueryRow(sb.String(), *args...)
	var res int64
	if err := row.Scan(&res); err != nil {
		return 0, err
	}
	return res, nil
}

func (d *DefaultMetricStorage) insertUpdatedDate(tsid int64, timeSeries TimeSeries) error {
	dates := map[string]struct{}{}
	for _, s := range timeSeries.Samples {
		date := time.Unix(s.Timestamp, 0).UTC().Format("2006-01-02")
		dates[date] = struct{}{}
	}

	var sb strings.Builder
	sb.WriteString("INSERT IGNORE INTO flash_metrics_update (tsid, updated_date) VALUES (?, ?)")
	for i := 0; i < len(dates)-1; i++ {
		sb.WriteString(", (?, ?)")
	}
	sb.WriteString(";")

	args := argSliceP.Get()
	defer argSliceP.Put(args)
	for d := range dates {
		*args = append(*args, tsid)
		*args = append(*args, d)
	}
	_, err := d.db.Exec(sb.String(), *args...)
	return err
}

func (d *DefaultMetricStorage) insertData(tsid int64, timeSeries TimeSeries) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO flash_metrics_data (tsid, ts, v) VALUES (?, ?, ?)")
	for i := 0; i < len(timeSeries.Samples)-1; i++ {
		sb.WriteString(", (?, ?, ?)")
	}
	sb.WriteString(";")

	args := argSliceP.Get()
	defer argSliceP.Put(args)
	for _, sample := range timeSeries.Samples {
		*args = append(*args, tsid)
		*args = append(*args, time.Unix(sample.Timestamp, 0).UTC().Format(time.RFC3339))
		*args = append(*args, sample.Value)
	}

	_, err := d.db.Exec(sb.String(), *args...)
	return err
}
