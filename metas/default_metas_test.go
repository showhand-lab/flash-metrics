package metas_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/showhand-lab/flash-metrics-storage/metas"
	"github.com/showhand-lab/flash-metrics-storage/utils"

	"github.com/stretchr/testify/suite"

	_ "github.com/go-sql-driver/mysql"
)

func TestDefaultMetas(t *testing.T) {
	if err := utils.PingTiDB(); err != nil {
		t.Skip("failed to ping database", err)
	}
	suite.Run(t, &testDefaultMetasSuite{})
}

type testDefaultMetasSuite struct {
	suite.Suite
	db *sql.DB
}

func (s *testDefaultMetasSuite) SetupSuite() {
	db, err := utils.SetupDB("test_default_metas")
	s.NoError(err)
	s.db = db
}

func (s *testDefaultMetasSuite) TearDownSuite() {
	s.NoError(utils.TearDownDB("test_default_metas", s.db))
}

func (s *testDefaultMetasSuite) TestBasic() {
	metaStorage := metas.NewDefaultMetaStorage(s.db)
	m, err := metaStorage.QueryMeta(context.Background(), "metric_a")
	s.NoError(err)
	s.Equal(m.MetricName, "metric_a")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{})

	m, err = metaStorage.StoreMeta(context.Background(), "metric_a", []string{"label_x"})
	s.NoError(err)
	s.Equal(m.MetricName, "metric_a")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{
		"label_x": 0,
	})

	m, err = metaStorage.QueryMeta(context.Background(), "metric_a")
	s.NoError(err)
	s.Equal(m.MetricName, "metric_a")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{
		"label_x": 0,
	})

	m, err = metaStorage.StoreMeta(context.Background(), "metric_a", []string{"label_y"})
	s.NoError(err)
	s.Equal(m.MetricName, "metric_a")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{
		"label_x": 0,
		"label_y": 1,
	})

	m, err = metaStorage.QueryMeta(context.Background(), "metric_a")
	s.NoError(err)
	s.Equal(m.MetricName, "metric_a")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{
		"label_x": 0,
		"label_y": 1,
	})

	m, err = metaStorage.StoreMeta(context.Background(), "metric_a", []string{"label_x"})
	s.NoError(err)
	s.Equal(m.MetricName, "metric_a")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{
		"label_x": 0,
		"label_y": 1,
	})

	m, err = metaStorage.StoreMeta(context.Background(), "metric_a", []string{"label_y"})
	s.NoError(err)
	s.Equal(m.MetricName, "metric_a")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{
		"label_x": 0,
		"label_y": 1,
	})

	m, err = metaStorage.StoreMeta(context.Background(), "metric_a", []string{"label_x", "label_y"})
	s.NoError(err)
	s.Equal(m.MetricName, "metric_a")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{
		"label_x": 0,
		"label_y": 1,
	})

	m, err = metaStorage.QueryMeta(context.Background(), "metric_b")
	s.NoError(err)
	s.Equal(m.MetricName, "metric_b")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{})

	// no cache affects
	anotherStorage := metas.NewDefaultMetaStorage(s.db)
	m, err = anotherStorage.QueryMeta(context.Background(), "metric_a")
	s.NoError(err)
	s.Equal(m.MetricName, "metric_a")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{
		"label_x": 0,
		"label_y": 1,
	})

	m, err = anotherStorage.QueryMeta(context.Background(), "metric_b")
	s.NoError(err)
	s.Equal(m.MetricName, "metric_b")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{})
}

func (s *testDefaultMetasSuite) TestLabelLimit() {
	metaStorage := metas.NewDefaultMetaStorage(s.db)
	m, err := metaStorage.QueryMeta(context.Background(), "metric_wide")
	s.NoError(err)
	s.Equal(m.MetricName, "metric_wide")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{})

	m, err = metaStorage.StoreMeta(context.Background(), "metric_wide", []string{
		"label-0",
		"label-1",
		"label-2",
		"label-3",
		"label-4",
		"label-5",
	})
	s.NoError(err)
	s.Equal(m.MetricName, "metric_wide")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{
		"label-0": 0,
		"label-1": 1,
		"label-2": 2,
		"label-3": 3,
		"label-4": 4,
		"label-5": 5,
	})

	_, err = metaStorage.StoreMeta(context.Background(), "metric_wide", []string{
		"label-6",
		"label-7",
		"label-8",
		"label-9",
		"label-10",
		"label-11",
		"label-12",
		"label-13",
		"label-14",
		"label-15",
	})
	s.Error(err)
	s.Contains(err.Error(), "exceed label limit")

	m, err = metaStorage.StoreMeta(context.Background(), "metric_wide", []string{
		"label-6",
		"label-7",
		"label-8",
		"label-9",
		"label-10",
		"label-11",
		"label-12",
		"label-13",
		"label-14",
	})
	s.NoError(err)
	s.Equal(m.MetricName, "metric_wide")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{
		"label-0":  0,
		"label-1":  1,
		"label-2":  2,
		"label-3":  3,
		"label-4":  4,
		"label-5":  5,
		"label-6":  6,
		"label-7":  7,
		"label-8":  8,
		"label-9":  9,
		"label-10": 10,
		"label-11": 11,
		"label-12": 12,
		"label-13": 13,
		"label-14": 14,
	})
}
