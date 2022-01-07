package store

import (
	"database/sql"
	"strconv"
	"strings"
	"time"

	"github.com/showhand-lab/flash-metrics-storage/metas"
)

var (
	interfaceSliceP = InterfaceSlicePool{}
)

type DefaultMetricStorage struct {
	*metas.DefaultMetaStorage

	DB *sql.DB
}

func NewDefaultMetricStorage(db *sql.DB) *DefaultMetricStorage {
	return &DefaultMetricStorage{DefaultMetaStorage: metas.NewDefaultMetaStorage(db), DB: db}
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

// Query implements interface MetricStorage
//
// SELECT
//    tsid, label0, label1, CAST(UNIX_TIMESTAMP(ts)*1000 AS UNSIGNED) AS t, v
//  FROM
//    flash_metrics_index
//    INNER JOIN flash_metrics_update ON (_tidb_rowid = tsid)
//    INNER JOIN flash_metrics_data USING (tsid)
//  WHERE
//    metric_name = "xxx"
//    AND label0 != "yyy"
//    AND label1 REGEXP "zzz.*"
//    AND DATE(start_ts) <= updated_date AND updated_date <= DATE(end_ts)
//    AND start_ts <= ts AND ts <= end_ts
//  ORDER BY tsid, t;
func (d *DefaultMetricStorage) Query(start, end int64, metricsName string, matchers []Matcher) ([]TimeSeries, error) {
	m, err := d.QueryMeta(metricsName)
	if err != nil {
		return nil, err
	}

	// Check query label exists. If contains non-exist label in matchers, return empty set.
	for _, matcher := range matchers {
		if _, ok := m.Labels[metas.LabelName(matcher.LabelName)]; !ok {
			return nil, nil
		}
	}

	args := interfaceSliceP.Get()
	defer interfaceSliceP.Put(args)

	var sb strings.Builder
	sb.WriteString("SELECT tsid, ")
	names := make([]string, 0, len(m.Labels))
	for n, v := range m.Labels {
		sb.WriteString("label")
		sb.WriteString(strconv.Itoa(int(v)))
		sb.WriteString(", ")
		names = append(names, string(n))
	}
	sb.WriteString("CAST(UNIX_TIMESTAMP(ts)*1000 AS UNSIGNED) AS t, v\n")
	sb.WriteString(`
FROM
  flash_metrics_index
  INNER JOIN flash_metrics_update ON (_tidb_rowid = tsid)
  INNER JOIN flash_metrics_data USING (tsid)
WHERE
  metric_name = ?
`)
	*args = append(*args, metricsName)

	for _, matcher := range matchers {
		labelID := m.Labels[metas.LabelName(matcher.LabelName)]
		sb.WriteString("AND label")
		sb.WriteString(strconv.Itoa(int(labelID)))

		if matcher.IsRE {
			if matcher.IsNegative {
				sb.WriteString(" NOT REGEXP ?\n")
			} else {
				sb.WriteString(" REGEXP ?\n")
			}
		} else {
			if matcher.IsNegative {
				sb.WriteString(" != ?\n")
			} else {
				sb.WriteString(" = ?\n")
			}
		}
		*args = append(*args, matcher.LabelValue)
	}

	sb.WriteString("AND ? <= updated_date AND updated_date <= ?\n")
	*args = append(*args, time.Unix(start/1000, (start%1000)*1_000_000).UTC().Format("2006-01-02"))
	*args = append(*args, time.Unix(end/1000, (end%1000)*1_000_000).UTC().Format("2006-01-02"))
	sb.WriteString("AND ? <= ts AND ts <= ?\n")
	sb.WriteString("ORDER BY tsid, t;")
	*args = append(*args, time.Unix(start/1000, (start%1000)*1_000_000).UTC().Format("2006-01-02 15:04:05.999 -0700"))
	*args = append(*args, time.Unix(end/1000, (end%1000)*1_000_000).UTC().Format("2006-01-02 15:04:05.999 -0700"))

	rows, err := d.DB.Query(sb.String(), *args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dest := interfaceSliceP.Get()
	defer interfaceSliceP.Put(dest)
	for i := 0; i < len(m.Labels)+3; i++ {
		*dest = append(*dest, nil)
	}

	destP := interfaceSliceP.Get()
	defer interfaceSliceP.Put(destP)
	for i := range *dest {
		*destP = append(*destP, &(*dest)[i])
	}

	var res []TimeSeries
	tsid := int64(0)
	var timeSeries *TimeSeries
	for rows.Next() {
		if err = rows.Scan(*destP...); err != nil {
			return nil, err
		}

		curTSID := (*dest)[0].(int64)
		if tsid != curTSID {
			tsid = curTSID
			res = append(res, TimeSeries{})
			timeSeries = &res[len(res)-1]
			timeSeries.Name = metricsName

			i := 1
			for _, name := range names {
				timeSeries.Labels = append(timeSeries.Labels, Label{
					Name:  name,
					Value: string((*dest)[i].([]byte)),
				})
				i += 1
			}
		}

		ts := (*dest)[len(*dest)-2].(int64)
		v := (*dest)[len(*dest)-1].(float64)
		timeSeries.Samples = append(timeSeries.Samples, Sample{
			TimestampMs: ts,
			Value:       v,
		})
	}

	return res, nil
}

// INSERT IGNORE INTO flash_metrics_index (metric_name, label0, label1) VALUES (?, ?, ?);
func (d *DefaultMetricStorage) insertIndex(timeSeries TimeSeries, m *metas.Meta) error {
	args := interfaceSliceP.Get()
	defer interfaceSliceP.Put(args)
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
	_, err := d.DB.Exec(sb.String(), *args...)
	return err
}

// SELECT _tidb_rowid FROM flash_metrics_index WHERE metric_name = ? AND label0 = ? AND label1 = ?;
func (d *DefaultMetricStorage) getTSID(timeSeries TimeSeries, m *metas.Meta) (int64, error) {
	args := interfaceSliceP.Get()
	defer interfaceSliceP.Put(args)
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
	row := d.DB.QueryRow(sb.String(), *args...)
	var res int64
	if err := row.Scan(&res); err != nil {
		return 0, err
	}
	return res, nil
}

// INSERT IGNORE INTO flash_metrics_update (tsid, updated_date) VALUES (?, ?), (?, ?), (?, ?);
func (d *DefaultMetricStorage) insertUpdatedDate(tsid int64, timeSeries TimeSeries) error {
	dates := map[string]struct{}{}
	for _, s := range timeSeries.Samples {
		date := time.Unix(s.TimestampMs/1000, (s.TimestampMs%1000)*1_000_000).UTC().Format("2006-01-02")
		dates[date] = struct{}{}
	}

	var sb strings.Builder
	sb.WriteString("INSERT IGNORE INTO flash_metrics_update (tsid, updated_date) VALUES (?, ?)")
	for i := 0; i < len(dates)-1; i++ {
		sb.WriteString(", (?, ?)")
	}
	sb.WriteString(";")

	args := interfaceSliceP.Get()
	defer interfaceSliceP.Put(args)
	for d := range dates {
		*args = append(*args, tsid)
		*args = append(*args, d)
	}
	_, err := d.DB.Exec(sb.String(), *args...)
	return err
}

// INSERT INTO flash_metrics_data (tsid, ts, v) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?);
func (d *DefaultMetricStorage) insertData(tsid int64, timeSeries TimeSeries) error {
	var sb strings.Builder
	sb.WriteString("INSERT INTO flash_metrics_data (tsid, ts, v) VALUES (?, ?, ?)")
	for i := 0; i < len(timeSeries.Samples)-1; i++ {
		sb.WriteString(", (?, ?, ?)")
	}
	sb.WriteString(";")

	args := interfaceSliceP.Get()
	defer interfaceSliceP.Put(args)
	for _, sample := range timeSeries.Samples {
		*args = append(*args, tsid)
		*args = append(*args, time.Unix(sample.TimestampMs/1000, (sample.TimestampMs%1000)*1_000_000).UTC().Format("2006-01-02 15:04:05.999 -0700"))
		*args = append(*args, sample.Value)
	}

	_, err := d.DB.Exec(sb.String(), *args...)
	return err
}
