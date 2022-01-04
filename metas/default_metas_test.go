package metas_test

import (
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/showhand-lab/flash-metrics-storage/metas"
	"github.com/showhand-lab/flash-metrics-storage/table"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestDefaultMetas(t *testing.T) {
	db, err := sql.Open("mysql", "root@(127.0.0.1:4000)/")
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

	suite.Run(t, &testDefaultMetasSuite{})
}

type testDefaultMetasSuite struct {
	suite.Suite

	db *sql.DB
}

func (s *testDefaultMetasSuite) SetupSuite() {
	db, err := sql.Open("mysql", "root@(127.0.0.1:4000)/")
	s.NoError(err)
	s.db = db

	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS test_default_metas")
	s.NoError(err)
	_, err = db.Exec("USE test_default_metas")
	s.NoError(err)

	_, err = db.Exec(table.CreateMeta)
	s.NoError(err)
}

func (s *testDefaultMetasSuite) TearDownSuite() {
	_, err := s.db.Exec("DROP DATABASE IF EXISTS test_default_metas")
	s.NoError(err)
	s.NoError(s.db.Close())
}

func (s *testDefaultMetasSuite) TestBasic() {
	metaStorage := metas.NewDefaultMetaStorage(s.db)
	m, err := metaStorage.QueryMeta("metric_a")
	s.NoError(err)
	s.Equal(m.MetricName, "metric_a")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{})

	m, err = metaStorage.StoreMeta("metric_a", []string{"label_x"})
	s.NoError(err)
	s.Equal(m.MetricName, "metric_a")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{
		"label_x": 0,
	})

	m, err = metaStorage.QueryMeta("metric_a")
	s.NoError(err)
	s.Equal(m.MetricName, "metric_a")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{
		"label_x": 0,
	})

	m, err = metaStorage.StoreMeta("metric_a", []string{"label_y"})
	s.NoError(err)
	s.Equal(m.MetricName, "metric_a")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{
		"label_x": 0,
		"label_y": 1,
	})

	m, err = metaStorage.QueryMeta("metric_a")
	s.NoError(err)
	s.Equal(m.MetricName, "metric_a")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{
		"label_x": 0,
		"label_y": 1,
	})

	m, err = metaStorage.StoreMeta("metric_a", []string{"label_x"})
	s.NoError(err)
	s.Equal(m.MetricName, "metric_a")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{
		"label_x": 0,
		"label_y": 1,
	})

	m, err = metaStorage.StoreMeta("metric_a", []string{"label_y"})
	s.NoError(err)
	s.Equal(m.MetricName, "metric_a")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{
		"label_x": 0,
		"label_y": 1,
	})

	m, err = metaStorage.StoreMeta("metric_a", []string{"label_x", "label_y"})
	s.NoError(err)
	s.Equal(m.MetricName, "metric_a")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{
		"label_x": 0,
		"label_y": 1,
	})

	m, err = metaStorage.QueryMeta("metric_b")
	s.NoError(err)
	s.Equal(m.MetricName, "metric_b")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{})

	// no cache affects
	anotherStorage := metas.NewDefaultMetaStorage(s.db)
	m, err = anotherStorage.QueryMeta("metric_a")
	s.NoError(err)
	s.Equal(m.MetricName, "metric_a")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{
		"label_x": 0,
		"label_y": 1,
	})

	m, err = anotherStorage.QueryMeta("metric_b")
	s.NoError(err)
	s.Equal(m.MetricName, "metric_b")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{})
}

func (s *testDefaultMetasSuite) TestLabelLimit() {
	metaStorage := metas.NewDefaultMetaStorage(s.db)
	m, err := metaStorage.QueryMeta("metric_wide")
	s.NoError(err)
	s.Equal(m.MetricName, "metric_wide")
	s.Equal(m.Labels, map[metas.LabelName]metas.LabelID{})

	m, err = metaStorage.StoreMeta("metric_wide", []string{
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

	m, err = metaStorage.StoreMeta("metric_wide", []string{
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

	m, err = metaStorage.StoreMeta("metric_wide", []string{
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
