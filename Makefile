PACKAGE_LIST  := go list ./...| grep -E "github.com/showhand-lab/flash-metrics/"
PACKAGE_LIST_TESTS  := go list ./... | grep -E "github.com/showhand-lab/flash-metrics/"
PACKAGES  ?= $$($(PACKAGE_LIST))
PACKAGES_TESTS ?= $$($(PACKAGE_LIST_TESTS))
PACKAGE_DIRECTORIES := $(PACKAGE_LIST) | sed 's|github.com/showhand-lab/flash-metrics/||'
FILES     := $$(find $$($(PACKAGE_DIRECTORIES)) -name "*.go")
FAIL_ON_STDOUT := awk '{ print } END { if (NR > 0) { exit 1 } }'

LDFLAGS += -X "github.com/showhand-lab/flash-metrics/utils/printer.FlashMetricsStorageBuildTS=$(shell date -u '+%Y-%m-%d %H:%M:%S')"
LDFLAGS += -X "github.com/showhand-lab/flash-metrics/utils/printer.FlashMetricsStorageGitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "github.com/showhand-lab/flash-metrics/utils/printer.FlashMetricsStorageGitBranch=$(shell git rev-parse --abbrev-ref HEAD)"

GO      := GO111MODULE=on go
GOBUILD := $(GO) build
GOTEST  := $(GO) test -p 8

default:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/flash-metrics ./main.go
	@echo Build successfully!

fmt:
	@echo "gofmt (simplify)"
	@gofmt -s -l -w . 2>&1 | $(FAIL_ON_STDOUT)
	@gofmt -s -l -w $(FILES) 2>&1 | $(FAIL_ON_STDOUT)

test:
	@echo "Running test"
	@export log_level=info; export TZ='Asia/Shanghai'; \
	$(GOTEST) -cover $(PACKAGES_TESTS) -coverprofile=coverage.txt

lint: golangci-lint revive import-lint

up-remote-write:
	@echo "Running prometheus for remote write"
	@rm -rf remote/remote_test/remote-write-data/
	@prometheus --config.file=remote/remote_test/prometheus_write.yml --storage.tsdb.path=remote/remote_test/remote-write-data --web.listen-address=0.0.0.0:9097

up-remote-read:
	@echo "Running prometheus for remote read"
	@rm -rf remote/remote_test/remote-read-data/
	@prometheus --config.file=remote/remote_test/prometheus_read.yml --storage.tsdb.path=remote/remote_test/remote-read-data --web.listen-address=0.0.0.0:9098

golangci-lint: tools/bin/golangci-lint
	@GO111MODULE=on tools/bin/golangci-lint run -v

tools/bin/golangci-lint:
	curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b ./tools/bin v1.41.1

revive: tools/bin/revive
	@tools/bin/revive -formatter friendly -config tools/check/revive.toml

tools/bin/revive: tools/check/go.mod
	@(cd tools/check && $(GO) build -o ../bin/revive github.com/mgechev/revive)

import-lint: tools/bin/go-import-lint
	@tools/bin/go-import-lint

tools/bin/go-import-lint: tools/check/go.mod
	@(cd tools/check && $(GO) build -o ../bin/go-import-lint github.com/hedhyw/go-import-lint/cmd/go-import-lint)
