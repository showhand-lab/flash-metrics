package config

import (
	"gopkg.in/yaml.v3"
	"os"
	"time"
)

type TiDBConfig struct {
	Address string `yaml:"address"`
}

type WebConfig struct {
	Address string `yaml:"address"`
}

type StaticConfig struct {
	Targets []string `yaml:"targets"`
}

type ScrapeConfig struct {
	JobName        string         `yaml:"job_name"`
	ScrapeInterval time.Duration  `yaml:"scrape_interval"`
	ScrapeTimeout  time.Duration  `yaml:"scrape_timeout"`
	MetricsPath    string         `yaml:"metrics_path"`
	Scheme         string         `yaml:"scheme"`
	StaticConfigs  []StaticConfig `yaml:"static_configs"`
}

type FlashMetricsConfig struct {
	TiDBConfig    TiDBConfig      `yaml:"tidb"`
	WebConfig     WebConfig       `yaml:"web"`
	ScrapeConfigs []*ScrapeConfig `yaml:"scrape_configs"`
}

var DefaultFlashMetricsConfig = FlashMetricsConfig{
	TiDBConfig: TiDBConfig{
		Address: "0.0.0.0:4000",
	},
	WebConfig: WebConfig{
		Address: "0.0.0.0:1200",
	},
}

func LoadConfig(cfgFilePath string) (*FlashMetricsConfig, error) {
	cfg := &DefaultFlashMetricsConfig
	file, err := os.Open(cfgFilePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// Init new YAML decode
	d := yaml.NewDecoder(file)

	// Start YAML decoding from file
	if err := d.Decode(&cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}
