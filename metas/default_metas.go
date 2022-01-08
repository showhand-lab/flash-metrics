package metas

import (
	"context"
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
	sync.Mutex

	db    *sql.DB
	cache *simplelru.LRU
}

func NewDefaultMetaStorage(db *sql.DB) *DefaultMetaStorage {
	cache, _ := simplelru.NewLRU(1024, nil)
	return &DefaultMetaStorage{db: db, cache: cache}
}

var _ MetaStorage = &DefaultMetaStorage{}

func (d *DefaultMetaStorage) QueryMeta(ctx context.Context, metricName string) (*Meta, error) {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	return d.queryMetaWithoutLock(ctx, metricName)
}

type LabelPair struct {
	Name LabelName
	ID   LabelID
}

func (d *DefaultMetaStorage) StoreMeta(ctx context.Context, metricName string, labelNames []string) (*Meta, error) {
	d.Mutex.Lock()
	defer d.Mutex.Unlock()

	r, err := d.queryMetaWithoutLock(ctx, metricName)
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
		newMeta := &Meta{
			MetricName: r.MetricName,
			Labels:     map[LabelName]LabelID{},
		}
		for n, id := range r.Labels {
			newMeta.Labels[n] = id
		}
		for _, newLabel := range *labels {
			newMeta.Labels[newLabel.Name] = newLabel.ID
		}
		r = newMeta

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

		_, err = d.db.ExecContext(ctx, sb.String(), *args...)
		if err != nil {
			return nil, err
		}
	}
	d.updateCacheWithoutLock(metricName, r)

	return r, nil
}

func (d *DefaultMetaStorage) queryMetaWithoutLock(ctx context.Context, metricName string) (*Meta, error) {
	if r := d.getMetaFromCacheWithoutLock(metricName); r != nil {
		return r, nil
	}

	rows, err := d.db.QueryContext(ctx, "SELECT label_name, label_id FROM flash_metrics_meta WHERE metric_name = ?", metricName)
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
	d.updateCacheWithoutLock(metricName, res)

	return res, nil
}

func (d *DefaultMetaStorage) getMetaFromCacheWithoutLock(metricName string) *Meta {
	if v, ok := d.cache.Get(metricName); ok {
		return v.(*Meta)
	}
	return nil
}

func (d *DefaultMetaStorage) updateCacheWithoutLock(metricName string, meta *Meta) {
	d.cache.Add(metricName, meta)
}
