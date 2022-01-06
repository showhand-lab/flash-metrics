package scrape

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/showhand-lab/flash-metrics-storage/config"
	"github.com/showhand-lab/flash-metrics-storage/store"

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

func MetricToTimeSeries(
	metricName string,
	metricType io_prometheus_client.MetricType,
	metric *io_prometheus_client.Metric,
	outerTimeSeries *[]store.TimeSeries) {

	nowMs := time.Now().UnixNano() / int64(time.Millisecond)

	labels := make([]store.Label, 0)
	for _, l := range metric.GetLabel() {
		labels = append(labels, store.Label{
			Name:  *l.Name,
			Value: *l.Value,
		})
	}
	var value float64
	switch metricType {
	case io_prometheus_client.MetricType_COUNTER:
		value = metric.Counter.GetValue()
		*outerTimeSeries = append(*outerTimeSeries, store.TimeSeries{
			Name:   metricName,
			Labels: labels,
			Samples: []store.Sample{{
				TimestampMs: nowMs,
				Value:       value,
			}},
		})
	case io_prometheus_client.MetricType_GAUGE:
		value = metric.Gauge.GetValue()
		*outerTimeSeries = append(*outerTimeSeries, store.TimeSeries{
			Name:   metricName,
			Labels: labels,
			Samples: []store.Sample{{
				TimestampMs: nowMs,
				Value:       value,
			}},
		})
	case io_prometheus_client.MetricType_UNTYPED:
		value = metric.Untyped.GetValue()
		*outerTimeSeries = append(*outerTimeSeries, store.TimeSeries{
			Name:   metricName,
			Labels: labels,
			Samples: []store.Sample{{
				TimestampMs: nowMs,
				Value:       value,
			}},
		})
	case io_prometheus_client.MetricType_SUMMARY:
		summary := metric.GetSummary()
		for _, quantile := range summary.GetQuantile() {
			quantileLabels := append(labels, store.Label{
				Name:  "quantile",
				Value: fmt.Sprintf("%v", quantile.GetQuantile()),
			})
			*outerTimeSeries = append(*outerTimeSeries, store.TimeSeries{
				Name:   metricName,
				Labels: quantileLabels,
				Samples: []store.Sample{{
					TimestampMs: nowMs,
					Value:       quantile.GetValue(),
				}},
			})
		}
		*outerTimeSeries = append(*outerTimeSeries, store.TimeSeries{
			Name:   metricName + "_sum",
			Labels: labels,
			Samples: []store.Sample{{
				TimestampMs: nowMs,
				Value:       *summary.SampleSum,
			}},
		})
		*outerTimeSeries = append(*outerTimeSeries, store.TimeSeries{
			Name:   metricName + "_count",
			Labels: labels,
			Samples: []store.Sample{{
				TimestampMs: nowMs,
				Value:       float64(*summary.SampleCount),
			}},
		})
	case io_prometheus_client.MetricType_HISTOGRAM:
		histogram := metric.GetHistogram()
		for _, bucket := range histogram.GetBucket() {
			histogramLabels := append(labels, store.Label{
				Name:  "le",
				Value: fmt.Sprintf("%v", bucket.GetUpperBound()),
			})
			*outerTimeSeries = append(*outerTimeSeries, store.TimeSeries{
				Name:   metricName + "_bucket",
				Labels: histogramLabels,
				Samples: []store.Sample{{
					TimestampMs: nowMs,
					Value:       float64(*bucket.CumulativeCount),
				}},
			})
		}
		*outerTimeSeries = append(*outerTimeSeries, store.TimeSeries{
			Name:   metricName + "_sum",
			Labels: labels,
			Samples: []store.Sample{{
				TimestampMs: nowMs,
				Value:       *histogram.SampleSum,
			}},
		})
		*outerTimeSeries = append(*outerTimeSeries, store.TimeSeries{
			Name:   metricName + "_count",
			Labels: labels,
			Samples: []store.Sample{{
				TimestampMs: nowMs,
				Value:       float64(*histogram.SampleCount),
			}},
		})
	default:
		log.Fatal("Unexpected metric type", zap.String("type", metricType.String()))
	}
}

// TODO: add test cases
// TODO: refactor duplicated snippets
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

	timeSeries := make([]store.TimeSeries, 0)
	for name, metricFamily := range metricFamilyMap {
		// TODO: support extra labels from scrape configs
		for _, metric := range metricFamily.GetMetric() {
			MetricToTimeSeries(name, metricFamily.GetType(), metric, &timeSeries)
		}
	}

	for _, tseries := range timeSeries {
		// TODO: 使用 batch store
		log.Debug("time series",
			zap.String("name", tseries.Name),
			zap.Int64("sample timestamp", tseries.Samples[0].TimestampMs),
			zap.Float64("sample value", tseries.Samples[0].Value))
		err := metricStore.Store(tseries)
		if err != nil {
			return
		}
	}
}
