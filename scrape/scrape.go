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

// TODO: test cases
// TODO: refactor duplicated snippets
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

	timeSeries := make([]store.TimeSeries, 0)
	nowMs := time.Now().UnixMilli()
	for name, metricFamily := range metricFamilyMap {
		for _, metric := range metricFamily.GetMetric() {
			labels := make([]store.Label, 0)
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
				timeSeries = append(timeSeries, store.TimeSeries{
					Name:   name,
					Labels: labels,
					Samples: []store.Sample{{
						TimestampMs: nowMs,
						Value:       value,
					}},
				})
			case io_prometheus_client.MetricType_GAUGE:
				value = metric.Gauge.GetValue()
				timeSeries = append(timeSeries, store.TimeSeries{
					Name:   name,
					Labels: labels,
					Samples: []store.Sample{{
						TimestampMs: nowMs,
						Value:       value,
					}},
				})
			case io_prometheus_client.MetricType_UNTYPED:
				value = metric.Untyped.GetValue()
				timeSeries = append(timeSeries, store.TimeSeries{
					Name:   name,
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
						Value: fmt.Sprintf("%f", quantile.GetQuantile()),
					})
					timeSeries = append(timeSeries, store.TimeSeries{
						Name:   name,
						Labels: quantileLabels,
						Samples: []store.Sample{{
							TimestampMs: nowMs,
							Value:       quantile.GetValue(),
						}},
					})
				}
				timeSeries = append(timeSeries, store.TimeSeries{
					Name:   name + "_sum",
					Labels: labels,
					Samples: []store.Sample{{
						TimestampMs: nowMs,
						Value:       *summary.SampleSum,
					}},
				})
				timeSeries = append(timeSeries, store.TimeSeries{
					Name:   name + "_count",
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
						Value: fmt.Sprintf("%f", bucket.GetUpperBound()),
					})
					timeSeries = append(timeSeries, store.TimeSeries{
						Name:   name,
						Labels: histogramLabels,
						Samples: []store.Sample{{
							TimestampMs: nowMs,
							Value:       float64(*bucket.CumulativeCount),
						}},
					})
				}

				timeSeries = append(timeSeries, store.TimeSeries{
					Name:   name + "_sum",
					Labels: labels,
					Samples: []store.Sample{{
						TimestampMs: nowMs,
						Value:       *histogram.SampleSum,
					}},
				})
				timeSeries = append(timeSeries, store.TimeSeries{
					Name:   name + "_count",
					Labels: labels,
					Samples: []store.Sample{{
						TimestampMs: nowMs,
						Value:       float64(*histogram.SampleCount),
					}},
				})
			default:
				log.Fatal("Unexpected metric type", zap.String("type", metricFamily.GetType().String()))
			}
		}
	}

	for _, tseries := range timeSeries {
		// TODO: 使用 batch store
		log.Info("time series",
			zap.String("name", tseries.Name),
			zap.Int64("sample timestamp", tseries.Samples[0].TimestampMs),
			zap.Float64("sample value", tseries.Samples[0].Value))
		err := metricStore.Store(tseries)
		if err != nil {
			return
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
