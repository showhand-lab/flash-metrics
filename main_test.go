package main_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBench(t *testing.T) {
	now := time.Now()

	for i := 0; i < 1000; i++ {

		args := fmt.Sprintf("query=sum%%28rate%%28tidb_server_query_total%%7Btidb_cluster%%3D%%22%%22%%7D%%5B30s%%5D%%29%%29+by+%%28result%%29&start=%d&end=%d&step=30", now.Unix()-3600, now.Unix())
		req, err := http.NewRequest("GET", "http://127.0.0.1:9977/api/v1/query_range", bytes.NewBuffer([]byte(args)))
		//req, err := http.NewRequest("GET", "http://127.0.0.1:9098/api/v1/query_range?" + args, nil)

		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		b, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		fmt.Println(string(b))
		err = resp.Body.Close()
		require.NoError(t, err)
	}

	fmt.Println("duration", time.Since(now))
}
