package metas_test

import (
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/showhand-lab/flash-metrics-storage/metas"
	"github.com/showhand-lab/flash-metrics-storage/table"
	"github.com/stretchr/testify/require"
)

func TestDefaultMetasBasic(t *testing.T) {
	db, err := sql.Open("mysql", "root@(127.0.0.1:4000)/test?parseTime=true")
	if err != nil {
		t.Skip("failed to open database", err)
	}
	defer func() {
		require.NoError(t, db.Close())
	}()

	err = db.Ping()
	if err != nil {
		t.Skip("failed to ping database", err)
	}

	_, err = db.Exec(table.DropMeta)
	require.NoError(t, err)
	_, err = db.Exec(table.CreateMeta)
	require.NoError(t, err)

	metaStorage := metas.NewDefaultMetaStorage(db)
	m, err := metaStorage.QueryMeta("metric_a")
	require.NoError(t, err)
	require.Equal(t, m.MetricName, "metric_a")
	require.Equal(t, m.Labels, map[metas.LabelName]metas.LabelID{})

	m, err = metaStorage.StoreMeta("metric_a", []string{"label_x"})
	require.NoError(t, err)
	require.Equal(t, m.MetricName, "metric_a")
	require.Equal(t, m.Labels, map[metas.LabelName]metas.LabelID{
		"label_x": 0,
	})

	m, err = metaStorage.QueryMeta("metric_a")
	require.NoError(t, err)
	require.Equal(t, m.MetricName, "metric_a")
	require.Equal(t, m.Labels, map[metas.LabelName]metas.LabelID{
		"label_x": 0,
	})

	m, err = metaStorage.StoreMeta("metric_a", []string{"label_y"})
	require.NoError(t, err)
	require.Equal(t, m.MetricName, "metric_a")
	require.Equal(t, m.Labels, map[metas.LabelName]metas.LabelID{
		"label_x": 0,
		"label_y": 1,
	})

	m, err = metaStorage.QueryMeta("metric_a")
	require.NoError(t, err)
	require.Equal(t, m.MetricName, "metric_a")
	require.Equal(t, m.Labels, map[metas.LabelName]metas.LabelID{
		"label_x": 0,
		"label_y": 1,
	})

	m, err = metaStorage.StoreMeta("metric_a", []string{"label_x"})
	require.NoError(t, err)
	require.Equal(t, m.MetricName, "metric_a")
	require.Equal(t, m.Labels, map[metas.LabelName]metas.LabelID{
		"label_x": 0,
		"label_y": 1,
	})

	m, err = metaStorage.StoreMeta("metric_a", []string{"label_y"})
	require.NoError(t, err)
	require.Equal(t, m.MetricName, "metric_a")
	require.Equal(t, m.Labels, map[metas.LabelName]metas.LabelID{
		"label_x": 0,
		"label_y": 1,
	})

	m, err = metaStorage.StoreMeta("metric_a", []string{"label_x", "label_y"})
	require.NoError(t, err)
	require.Equal(t, m.MetricName, "metric_a")
	require.Equal(t, m.Labels, map[metas.LabelName]metas.LabelID{
		"label_x": 0,
		"label_y": 1,
	})

	m, err = metaStorage.QueryMeta("metric_b")
	require.NoError(t, err)
	require.Equal(t, m.MetricName, "metric_b")
	require.Equal(t, m.Labels, map[metas.LabelName]metas.LabelID{})

	// no cache affects
	anotherStorage := metas.NewDefaultMetaStorage(db)
	m, err = anotherStorage.QueryMeta("metric_a")
	require.NoError(t, err)
	require.Equal(t, m.MetricName, "metric_a")
	require.Equal(t, m.Labels, map[metas.LabelName]metas.LabelID{
		"label_x": 0,
		"label_y": 1,
	})

	m, err = anotherStorage.QueryMeta("metric_b")
	require.NoError(t, err)
	require.Equal(t, m.MetricName, "metric_b")
	require.Equal(t, m.Labels, map[metas.LabelName]metas.LabelID{})
}
