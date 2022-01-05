package printer

import (
	"github.com/pingcap/log"
	"go.uber.org/zap"

	_ "runtime" // import link package
	_ "unsafe"  // required by go:linkname
)

// Version information.
var (
	FlashMetricsStorageBuildTS   = "None"
	FlashMetricsStorageGitHash   = "None"
	FlashMetricsStorageGitBranch = "None"
)

//go:linkname buildVersion runtime.buildVersion
var buildVersion string

// PrintFlashMetricsStorageInfo prints the FlashMetricsStorage version information.
func PrintFlashMetricsStorageInfo() {
	log.Info("Welcome to flash-metrics",
		zap.String("Git Commit Hash", FlashMetricsStorageGitHash),
		zap.String("Git Branch", FlashMetricsStorageGitBranch),
		zap.String("UTC Build Time", FlashMetricsStorageBuildTS),
		zap.String("GoVersion", buildVersion))
}
