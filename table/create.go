package table

const (
	CreateData = `
CREATE TABLE IF NOT EXISTS flash_metrics_data (
    tsid bigint NOT NULL,
    ts TIMESTAMP NOT NULL,
    v DOUBLE
);
`

	CreateIndex = `
CREATE TABLE IF NOT EXISTS flash_metrics_index (
    metric_name CHAR(255) NOT NULL,
    label0 CHAR(255),
    label1 CHAR(255),
    PRIMARY KEY (metric_name, label0, label1)
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
CREATE TABLE flash_metrics_meta (
    metric_name CHAR(255) NOT NULL,
    label_name CHAR(255) NOT NULL,
    label_id TINYINT NOT NULL,
    PRIMARY KEY (metric_name, label_name)
);
`
)
