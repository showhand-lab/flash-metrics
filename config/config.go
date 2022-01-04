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
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

type ScrapeConfig struct {
	JobName        string        `yaml:"job_name"`
	ScrapeInterval time.Duration `yaml:"scrape_interval"`
	ScrapeTimeout  time.Duration `yaml:"scrape_timeout"`
	MetricsPath    string        `yaml:"metrics_path"`
	Scheme         string        `yaml:"scheme"`
	StaticConfigs  []struct {
		Targets []string `yaml:"targets"`
	} `yaml:"static_configs"`
}

type FlashMetricsConfig struct {
	TiDBConfig   TiDBConfig     `yaml:"tidb"`
	WebConfig    WebConfig      `yaml:"web"`
	ScrapeConfig []ScrapeConfig `yaml:"scrape_configs"`
}

var DefaultFlashMetricsConfig = FlashMetricsConfig{
	TiDBConfig: TiDBConfig{
		Address: "0.0.0.0:4000",
	},
	WebConfig: WebConfig{
		Host: "0.0.0.0",
		Port: 1200,
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
