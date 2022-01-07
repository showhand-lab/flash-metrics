package parser

import (
	"bufio"
	"fmt"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func TestQueryParse(t *testing.T) {
	fi, err := os.Open("./testdata/tidb-metrics.in")
	if err != nil {
		log.Fatal("open file failed", zap.Error(err))
		return
	}
	defer fi.Close()

	br := bufio.NewReader(fi)

	cnt := 0
	skipped := 0

	for {
		raw, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}
		line := string(raw)

		if strings.HasPrefix(line, "#") || line == "" {
			log.Info("skip promql" + line)
			skipped++
			continue
		}

		_, err = NewInstantQuery(nil, line, time.Now())
		if err != nil {
			t.Fatalf("already pass %d promqls", cnt)
		}
		cnt++
	}
	fmt.Printf("well done, %v promql passed, %v promql skipped", cnt, skipped)
}