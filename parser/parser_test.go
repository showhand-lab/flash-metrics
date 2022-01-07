package parser

import (
	"bufio"
	"go.uber.org/zap"
	"io"
	"log"
	"os"
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

	for {
		raw, _, err := br.ReadLine()
		if err == io.EOF {
			break
		}

		_, err = NewInstantQuery(nil, string(raw), time.Now())
		if err != nil {
			t.Fatalf("already pass %d promqls", cnt)
		}
		cnt++
	}
}