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

func scrapeLoop(ctx context.Context, scrapeConfig *config.ScrapeConfig, metricStore store.MetricStorage) {
	ticker := time.NewTicker(scrapeConfig.ScrapeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for _, staticConfig := range scrapeConfig.StaticConfigs {
				for _, target := range staticConfig.Targets {
					wg.Add(1)
					go func(target string) {
						ctx, cancel := context.WithTimeout(ctx, scrapeConfig.ScrapeTimeout)
						defer func() {
							cancel()
							wg.Done()
						}()
						scrapeTarget(
							ctx,
							&http.Client{Timeout: scrapeConfig.ScrapeTimeout},
							fmt.Sprintf("%s://%s%s", scrapeConfig.Scheme, target, scrapeConfig.MetricsPath),
							metricStore,
						)
					}(target)
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

// TODO: add test cases
// TODO: refactor duplicated snippets
// TODO: store metric type, not just time series
func scrapeTarget(ctx context.Context, httpClient *http.Client, targetUrl string, metricStore store.MetricStorage) {
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

	timeSeries := make([]*model.TimeSeries, 0)
	nowMs := time.Now().UnixNano() / int64(time.Millisecond)
	for name, metricFamily := range metricFamilyMap {

		// TODO: support extra labels from scrape configs
		for _, metric := range metricFamily.GetMetric() {
			labels := make([]model.Label, 0)
			for _, l := range metric.GetLabel() {
				labels = append(labels, model.Label{
					Name:  *l.Name,
					Value: *l.Value,
				})
			}
			var value float64
			switch metricFamily.GetType() {
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
						Value: fmt.Sprintf("%f", quantile.GetQuantile()),
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
						Value: fmt.Sprintf("%f", bucket.GetUpperBound()),
					})
					timeSeries = append(timeSeries, &model.TimeSeries{
						Name:   name,
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
				log.Fatal("Unexpected metric type", zap.String("type", metricFamily.GetType().String()))
			}
		}
	}

	if err = metricStore.BatchStore(context.Background(), timeSeries); err != nil {
		log.Warn("failed to batch store time series", zap.Error(err))
	}
}
