package scrape

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/showhand-lab/flash-metrics-storage/config"
	"github.com/showhand-lab/flash-metrics-storage/store"
	"github.com/showhand-lab/flash-metrics-storage/store/model"

	"github.com/pingcap/log"
	io_prometheus_client "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"go.uber.org/zap"
)

var (
	wg           sync.WaitGroup
	cancelScrape context.CancelFunc
)

func Init(flashMetricsConfig *config.FlashMetricsConfig, metricStore store.MetricStorage) {
	ctx, cancel := context.WithCancel(context.Background())
	cancelScrape = cancel
	for _, scrapeConfig := range flashMetricsConfig.ScrapeConfigs {
		wg.Add(1)
		go func(scrapeConfig *config.ScrapeConfig) {
			defer wg.Done()
			scrapeLoop(ctx, scrapeConfig, metricStore)
		}(scrapeConfig)
	}
}

func Stop() {
	cancelScrape()
	wg.Wait()
}

func storeTimeSeries(ctx context.Context, metricStore store.MetricStorage, timeSeries []*model.TimeSeries) {
	if err := metricStore.BatchStore(ctx, timeSeries); err != nil {
		log.Warn("failed to batch store time series", zap.Error(err))
	}
}

func scrapeLoop(ctx context.Context, scrapeConfig *config.ScrapeConfig, metricStore store.MetricStorage) {
	ticker := time.NewTicker(scrapeConfig.ScrapeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for _, staticConfig := range scrapeConfig.StaticConfigs {
				for _, targetInstance := range staticConfig.Targets {
					wg.Add(1)
					defaultLabels := []model.Label{
						{Name: "job", Value: scrapeConfig.JobName},
						{Name: "instance", Value: targetInstance},
					}
					log.Info("start scraping",
						zap.String("job", scrapeConfig.JobName),
						zap.String("instance", targetInstance))
					go func(targetInstance string, defaultLabels *[]model.Label) {
						ctx, cancel := context.WithTimeout(ctx, scrapeConfig.ScrapeTimeout)
						defer func() {
							cancel()
							wg.Done()
						}()

						err, timeSeries := scrapeTarget(
							ctx,
							scrapeConfig,
							targetInstance,
							defaultLabels,
						)

						if err != nil {
							log.Error("fail to scrape", zap.Error(err))
							return
						}
						storeTimeSeries(ctx, metricStore, timeSeries)

					}(targetInstance, &defaultLabels)
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// TODO: add test cases
// TODO: refactor duplicated snippets
func scrapeTarget(
	ctx context.Context,
	scrapeConfig *config.ScrapeConfig,
	targetInstance string,
	defaultLabels *[]model.Label) (err error, timeSeries []*model.TimeSeries) {

	now := time.Now()
	defer func() {
		// Add default time series at the of scraping this target
		// https://prometheus.io/docs/concepts/jobs_instances/#automatically-generated-labels-and-time-series
		isInstanceHealthy := 1.0
		scrapeFinishTime := time.Now()
		scrapeFinishTimeMs := scrapeFinishTime.UnixNano() / int64(time.Millisecond)
		scrapedSampleCount := len(timeSeries)

		if err != nil {
			isInstanceHealthy = 0
		}
		timeSeries = append(timeSeries, &model.TimeSeries{
			Name:   "up",
			Labels: *defaultLabels,
			Samples: []model.Sample{
				{TimestampMs: scrapeFinishTimeMs, Value: isInstanceHealthy},
			},
		})
		timeSeries = append(timeSeries, &model.TimeSeries{
			Name:   "scrape_duration_seconds",
			Labels: *defaultLabels,
			Samples: []model.Sample{
				{TimestampMs: scrapeFinishTimeMs, Value: float64(scrapeFinishTime.Second() - now.Second())},
			},
		})
		timeSeries = append(timeSeries, &model.TimeSeries{
			Name:   "scrape_samples_scraped",
			Labels: *defaultLabels,
			Samples: []model.Sample{
				{TimestampMs: scrapeFinishTimeMs, Value: float64(scrapedSampleCount)},
			},
		})
	}()

	targetUrl := fmt.Sprintf("%s://%s%s", scrapeConfig.Scheme, targetInstance, scrapeConfig.MetricsPath)
	httpClient := &http.Client{Timeout: scrapeConfig.ScrapeTimeout}
	req, err := http.NewRequestWithContext(ctx, "GET", targetUrl, nil)
	if err != nil {
		log.Warn("failed to create request", zap.Error(err))
		return
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		log.Warn("failed to do request", zap.Error(err))
		return
	}
	defer resp.Body.Close()

	var textMetricParser expfmt.TextParser
	metricFamilyMap, err := textMetricParser.TextToMetricFamilies(resp.Body)
	if err != nil {
		log.Error(err.Error(), zap.String("target", targetUrl))
		return
	}

	nowMs := now.UnixNano() / int64(time.Millisecond)
	for name, metricFamily := range metricFamilyMap {
		// TODO: support extra labels from scrape configs
		for _, metric := range metricFamily.GetMetric() {
			labels := make([]model.Label, len(*defaultLabels))
			copy(labels, *defaultLabels)

			for _, l := range metric.GetLabel() {
				labels = append(labels, model.Label{
					Name:  *l.Name,
					Value: *l.Value,
				})
			}
			var value float64
			metricType := metricFamily.GetType()
			switch metricType {
			case io_prometheus_client.MetricType_COUNTER:
				value = metric.Counter.GetValue()
				timeSeries = append(timeSeries, &model.TimeSeries{
					Name:   name,
					Labels: labels,
					Samples: []model.Sample{{
						TimestampMs: nowMs,
						Value:       value,
					}},
				})
			case io_prometheus_client.MetricType_GAUGE:
				value = metric.Gauge.GetValue()
				timeSeries = append(timeSeries, &model.TimeSeries{
					Name:   name,
					Labels: labels,
					Samples: []model.Sample{{
						TimestampMs: nowMs,
						Value:       value,
					}},
				})
			case io_prometheus_client.MetricType_UNTYPED:
				value = metric.Untyped.GetValue()
				timeSeries = append(timeSeries, &model.TimeSeries{
					Name:   name,
					Labels: labels,
					Samples: []model.Sample{{
						TimestampMs: nowMs,
						Value:       value,
					}},
				})
			case io_prometheus_client.MetricType_SUMMARY:
				summary := metric.GetSummary()
				for _, quantile := range summary.GetQuantile() {
					quantileLabels := append(labels, model.Label{
						Name:  "quantile",
						Value: fmt.Sprintf("%v", quantile.GetQuantile()),
					})
					timeSeries = append(timeSeries, &model.TimeSeries{
						Name:   name,
						Labels: quantileLabels,
						Samples: []model.Sample{{
							TimestampMs: nowMs,
							Value:       quantile.GetValue(),
						}},
					})
				}
				timeSeries = append(timeSeries, &model.TimeSeries{
					Name:   name + "_sum",
					Labels: labels,
					Samples: []model.Sample{{
						TimestampMs: nowMs,
						Value:       *summary.SampleSum,
					}},
				})
				timeSeries = append(timeSeries, &model.TimeSeries{
					Name:   name + "_count",
					Labels: labels,
					Samples: []model.Sample{{
						TimestampMs: nowMs,
						Value:       float64(*summary.SampleCount),
					}},
				})
			case io_prometheus_client.MetricType_HISTOGRAM:
				histogram := metric.GetHistogram()
				for _, bucket := range histogram.GetBucket() {
					histogramLabels := append(labels, model.Label{
						Name:  "le",
						Value: fmt.Sprintf("%v", bucket.GetUpperBound()),
					})
					timeSeries = append(timeSeries, &model.TimeSeries{
						Name:   name + "_bucket",
						Labels: histogramLabels,
						Samples: []model.Sample{{
							TimestampMs: nowMs,
							Value:       float64(*bucket.CumulativeCount),
						}},
					})
				}
				timeSeries = append(timeSeries, &model.TimeSeries{
					Name:   name + "_sum",
					Labels: labels,
					Samples: []model.Sample{{
						TimestampMs: nowMs,
						Value:       *histogram.SampleSum,
					}},
				})
				timeSeries = append(timeSeries, &model.TimeSeries{
					Name:   name + "_count",
					Labels: labels,
					Samples: []model.Sample{{
						TimestampMs: nowMs,
						Value:       float64(*histogram.SampleCount),
					}},
				})
			default:
				log.Fatal("Unexpected metric type", zap.String("type", metricType.String()))
			}
		}
	}
	return
}
