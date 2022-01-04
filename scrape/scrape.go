package scrape

import (
	"context"
	"fmt"
	"github.com/pingcap/log"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/showhand-lab/flash-metrics-storage/config"
	"github.com/showhand-lab/flash-metrics-storage/store"
	"go.uber.org/zap"
	"net/http"
	"time"
)

var scrapeLoop struct {
	ctx           context.Context
	metricStore   store.MetricStorage
	ScrapeConfigs *config.FlashMetricsConfig
}

var jobsEventChan = make(chan *config.ScrapeConfig)

func scrapeJob(ctx context.Context, scrapeConfig *config.ScrapeConfig) {
	ticker := time.NewTicker(scrapeConfig.ScrapeInterval)
	for {
		select {
		case <-ticker.C:
			jobsEventChan <- scrapeConfig
		case <-ctx.Done():
			ticker.Stop()
			return
		}
	}
}

func Init(ctx context.Context, metricStore store.MetricStorage, flashMetricsConfig *config.FlashMetricsConfig) {
	scrapeLoop.ctx = ctx
	scrapeLoop.metricStore = metricStore
	scrapeLoop.ScrapeConfigs = flashMetricsConfig

	for _, scrapeConfig := range flashMetricsConfig.ScrapeConfigs {
		go scrapeJob(ctx, scrapeConfig)
	}

	for {
		select {
		case event := <-jobsEventChan:
			go scrape(metricStore, event)
		case <-ctx.Done():
			close(jobsEventChan)
			return
		}
	}
}

// FIXME: 还没测试，应该还需要 goroutine 上的修改
func scrapeTarget(metricStore store.MetricStorage, url string) {
	resp, err := http.Get(url)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer resp.Body.Close()

	var textMetricParser expfmt.TextParser
	metricFamilyMap, err := textMetricParser.TextToMetricFamilies(resp.Body)

	for name, metricFamily := range metricFamilyMap {
		for _, metric := range metricFamily.GetMetric() {
			labels := make([]store.Label, len(metric.GetLabel()))
			for _, l := range metric.GetLabel() {
				labels = append(labels, store.Label{
					Name:  *l.Name,
					Value: *l.Value,
				})
			}
			var value float64
			switch metricFamily.GetType() {
			case io_prometheus_client.MetricType_COUNTER:
				value = metric.Counter.GetValue()
			}

			// FIXME: 不知道怎样对应出来多个 samples
			samples := make([]store.Sample, 1)
			samples = append(samples, store.Sample{
				Timestamp: time.Now().Unix(),
				Value:     value,
			})

			timeSeries := store.TimeSeries{
				Name:    name,
				Labels:  labels,
				Samples: samples,
			}
			log.Info("time series",
				zap.String("name", timeSeries.Name),
				zap.Int64("sample timestamp", timeSeries.Samples[0].Timestamp),
				zap.Float64("sample value", timeSeries.Samples[0].Value))
			err := metricStore.Store(timeSeries)
			if err != nil {
				return
			}
		}

	}
}

func scrape(metricStore store.MetricStorage, scrapeConfig *config.ScrapeConfig) {
	for _, staticConfig := range scrapeConfig.StaticConfigs {
		for _, target := range staticConfig.Targets {
			go scrapeTarget(
				metricStore,
				fmt.Sprintf("%s://%s%s", scrapeConfig.Scheme, target, scrapeConfig.MetricsPath),
			)
		}
	}
}
