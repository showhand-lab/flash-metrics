package printer

import (
	"github.com/pingcap/log"
	"go.uber.org/zap"

	_ "runtime" // import link package
	_ "unsafe"  // required by go:linkname
)

// Version information.
var (
	FlashMetricsBuildTS   = "None"
	FlashMetricsGitHash   = "None"
	FlashMetricsGitBranch = "None"
)

//go:linkname buildVersion runtime.buildVersion
var buildVersion string

// PrintFlashMetricsInfo prints the FlashMetrics version information.
func PrintFlashMetricsInfo() {
	log.Info("Welcome to flash-metrics",
		zap.String("Git Commit Hash", FlashMetricsGitHash),
		zap.String("Git Branch", FlashMetricsGitBranch),
		zap.String("UTC Build Time", FlashMetricsBuildTS),
		zap.String("GoVersion", buildVersion))
}
