package table

const (
	CreateData = `
CREATE TABLE IF NOT EXISTS flash_metrics_data (
    tsid bigint NOT NULL,
    ts TIMESTAMP(3) NOT NULL,
    v DOUBLE
);
`

	MaxLabelCount = 15
	CreateIndex   = `
CREATE TABLE IF NOT EXISTS flash_metrics_index (
    metric_name CHAR(128) NOT NULL,
    label0 CHAR(128),
    label1 CHAR(128),
    label2 CHAR(128),
    label3 CHAR(128),
    label4 CHAR(128),
    label5 CHAR(128),
    label6 CHAR(128),
    label7 CHAR(128),
    label8 CHAR(128),
    label9 CHAR(128),
    label10 CHAR(128),
    label11 CHAR(128),
    label12 CHAR(128),
    label13 CHAR(128),
    label14 CHAR(128),
    PRIMARY KEY (metric_name, label0, label1,
      label2, label3, label4, label5, label6,
      label7, label8, label9, label10, label11,
      label12, label13, label14)
);
`

	CreateUpdate = `
CREATE TABLE IF NOT EXISTS flash_metrics_update (
    tsid bigint NOT NULL,
    updated_date DATE NOT NULL,
    PRIMARY KEY (tsid, updated_date) CLUSTERED
);
`

	CreateMeta = `
CREATE TABLE IF NOT EXISTS flash_metrics_meta (
    metric_name VARCHAR(255) NOT NULL,
    label_name VARCHAR(255) NOT NULL,
    label_id TINYINT NOT NULL,
    PRIMARY KEY (metric_name, label_name)
);
`
)
