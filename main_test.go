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

	body := bytes.NewBuffer([]byte("query=sum%28rate%28tidb_server_query_total%7Btidb_cluster%3D%22%22%7D%5B30s%5D%29%29+by+%28result%29&start=1641617880&end=1641621480&step=30"))
	req, err := http.NewRequest("GET", "http://127.0.0.1:9977/api/v1/query_range", body)
	//req, err := http.NewRequest("GET", "http://127.0.0.1:9090/api/v1/query_range?query=sum%28rate%28tidb_server_query_total%7Btidb_cluster%3D%22%22%7D%5B30s%5D%29%29+by+%28result%29&start=1641617880&end=1641621480&step=30", nil)

	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	b, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	fmt.Println(string(b))
	err = resp.Body.Close()
	require.NoError(t, err)

	fmt.Println("duration", time.Since(now))
}
