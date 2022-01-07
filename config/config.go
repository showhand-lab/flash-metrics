package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
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

type LogConfig struct {
	LogLevel string `yaml:"log_level"`
	LogFile  string `yaml:"log_file"`
}

type FlashMetricsConfig struct {
	TiDBConfig    TiDBConfig      `yaml:"tidb"`
	WebConfig     WebConfig       `yaml:"web"`
	ScrapeConfigs []*ScrapeConfig `yaml:"scrape_configs"`
	LogConfig     LogConfig       `yaml:"logs"`
}

var DefaultFlashMetricsConfig = FlashMetricsConfig{
	TiDBConfig: TiDBConfig{
		Address: "127.0.0.1:4000",
	},
	WebConfig: WebConfig{
		Address: "127.0.0.1:9977",
	},
	LogConfig: LogConfig{
		LogLevel: "info",
	},
}

func LoadConfig(cfgFilePath string, override func(config *FlashMetricsConfig)) (*FlashMetricsConfig, error) {
	cfg := DefaultFlashMetricsConfig

	if cfgFilePath != "" {
		file, err := os.Open(cfgFilePath)
		if err != nil {
			return nil, err
		}
		defer file.Close()

		// Init new YAML decode
		d := yaml.NewDecoder(file)

		// Start YAML decoding from file
		if err = d.Decode(&cfg); err != nil {
			return nil, err
		}
	}

	override(&cfg)
	return &cfg, nil
}
