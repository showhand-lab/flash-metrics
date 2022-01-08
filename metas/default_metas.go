package metas

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/showhand-lab/flash-metrics/table"

	"github.com/hashicorp/golang-lru/simplelru"
)

var (
	labelPairSliceP = LabelPairSlicePool{}
	argSliceP       = ArgSlicePool{}
)

type DefaultMetaStorage struct {
	db *sql.DB

	cacheMu sync.Mutex
	cache   *simplelru.LRU
}

func NewDefaultMetaStorage(db *sql.DB) *DefaultMetaStorage {
	cache, _ := simplelru.NewLRU(1024, nil)
	return &DefaultMetaStorage{db: db, cache: cache}
}

var _ MetaStorage = &DefaultMetaStorage{}

func (d *DefaultMetaStorage) QueryMeta(metricName string) (*Meta, error) {
	if r := d.getMetaFromCache(metricName); r != nil {
		return r, nil
	}

	rows, err := d.db.Query("SELECT label_name, label_id FROM flash_metrics_meta WHERE metric_name = ?", metricName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := &Meta{
		MetricName: metricName,
		Labels:     map[LabelName]LabelID{},
	}
	for rows.Next() {
		var name LabelName
		var id LabelID
		// for each row, scan the result into our tag composite object
		err = rows.Scan(&name, &id)
		if err != nil {
			return nil, err
		}
		res.Labels[name] = id
	}
	return res, nil
}

type LabelPair struct {
	Name LabelName
	ID   LabelID
}

func (d *DefaultMetaStorage) StoreMeta(metricName string, labelNames []string) (*Meta, error) {
	r, err := d.QueryMeta(metricName)
	if err != nil {
		return nil, err
	}

	labels := labelPairSliceP.Get()
	defer labelPairSliceP.Put(labels)

	newID := len(r.Labels)
	for _, labelName := range labelNames {
		if _, ok := r.Labels[LabelName(labelName)]; ok {
			continue
		}

		*labels = append(*labels, LabelPair{
			Name: LabelName(labelName),
			ID:   LabelID(newID),
		})
		newID += 1
	}

	newLabelLen := len(*labels)
	if newLabelLen+len(r.Labels) > table.MaxLabelCount {
		return nil, fmt.Errorf("failed to add new labels for %s due to exceed label limit: %d", metricName, table.MaxLabelCount)
	}

	if newLabelLen > 0 {
		args := argSliceP.Get()
		defer argSliceP.Put(args)
		for _, newLabel := range *labels {
			*args = append(*args, metricName)
			*args = append(*args, newLabel.Name)
			*args = append(*args, newLabel.ID)
		}

		var sb strings.Builder
		sb.WriteString("INSERT INTO flash_metrics_meta VALUES (?, ?, ?)")
		for i := 0; i < newLabelLen-1; i++ {
			sb.WriteString(", (?, ?, ?)")
		}
		sb.WriteByte(';')

		_, err = d.db.Exec(sb.String(), *args...)
		if err != nil {
			return nil, err
		}

		for _, newLabel := range *labels {
			r.Labels[newLabel.Name] = newLabel.ID
		}
	}
	d.updateCache(metricName, r)

	return r, nil
}

func (d *DefaultMetaStorage) getMetaFromCache(metricName string) *Meta {
	d.cacheMu.Lock()
	defer d.cacheMu.Unlock()
	if v, ok := d.cache.Get(metricName); ok {
		return v.(*Meta)
	}
	return nil
}

func (d *DefaultMetaStorage) updateCache(metricName string, meta *Meta) {
	d.cacheMu.Lock()
	d.cache.Add(metricName, meta)
	d.cacheMu.Unlock()
}
