package table

const (
	AlterTiflashData   = "ALTER TABLE flash_metrics_data SET TIFLASH REPLICA 1;"
	AlterTiflashIndex  = "ALTER TABLE flash_metrics_index SET TIFLASH REPLICA 1;"
	AlterTiflashUpdate = "ALTER TABLE flash_metrics_update SET TIFLASH REPLICA 1;"
)
