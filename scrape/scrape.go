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
	"sync"
	"time"
)

type scrapeEvent struct {
	httpClient   *http.Client
	scrapeConfig *config.ScrapeConfig
}

func newScrapeEvent(cfg *config.ScrapeConfig) *scrapeEvent {
	return &scrapeEvent{
		httpClient:   &http.Client{Timeout: cfg.ScrapeTimeout},
		scrapeConfig: cfg,
	}
}

var jobsEventChan = make(chan *scrapeEvent)

func scrapeJob(ctx context.Context, scrapeConfig *config.ScrapeConfig) {
	ticker := time.NewTicker(scrapeConfig.ScrapeInterval)
	relatedScrapeEvent := newScrapeEvent(scrapeConfig)
	for {
		select {
		case <-ticker.C:
			jobsEventChan <- relatedScrapeEvent
		case <-ctx.Done():
			ticker.Stop()
			return
		}
	}
}

func Init(ctx context.Context, metricStore store.MetricStorage, flashMetricsConfig *config.FlashMetricsConfig) {
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

func scrapeTarget(wg *sync.WaitGroup, httpClient *http.Client, targetUrl string, metricStore store.MetricStorage) {
	defer wg.Done()
	resp, err := httpClient.Get(targetUrl)
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
			case io_prometheus_client.MetricType_GAUGE:
				value = metric.Gauge.GetValue()
			case io_prometheus_client.MetricType_UNTYPED:
				value = metric.Untyped.GetValue()
				// TODO: 确定 Summary 和 Histogram 怎么存储
				//case io_prometheus_client.MetricType_SUMMARY:
				//	value = metric.Summary.Quantile
				//case io_prometheus_client.MetricType_HISTOGRAM:
				//	value = metric.Histogram.Bucket
			}

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

			// TODO: 放在外循环，然后使用 batch store
			err := metricStore.Store(timeSeries)
			if err != nil {
				return
			}
		}

	}
}

func scrape(metricStore store.MetricStorage, scrapeEvent *scrapeEvent) {
	wg := sync.WaitGroup{}
	scrapeConfig := scrapeEvent.scrapeConfig
	for _, staticConfig := range scrapeConfig.StaticConfigs {
		for _, target := range staticConfig.Targets {
			wg.Add(1)
			go scrapeTarget(
				&wg,
				scrapeEvent.httpClient,
				fmt.Sprintf("%s://%s%s", scrapeConfig.Scheme, target, scrapeConfig.MetricsPath),
				metricStore,
			)
		}
	}
	wg.Wait()
}
