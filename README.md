# Flash Metrics Storage

```shell
$ echo -e "max-index-length = 12288" > tidb.config

$ tiup playground nightly --db.config tidb.config

$ make test
Running test
ok      github.com/showhand-lab/flash-metrics-storage/metas     1.157s  coverage: 94.3% of statements
ok      github.com/showhand-lab/flash-metrics-storage/remote    2.419s  coverage: 72.4% of statements
?       github.com/showhand-lab/flash-metrics-storage/service   [no test files]
?       github.com/showhand-lab/flash-metrics-storage/service/http      [no test files]
ok      github.com/showhand-lab/flash-metrics-storage/store     1.640s  coverage: 93.5% of statements
?       github.com/showhand-lab/flash-metrics-storage/table     [no test files]
?       github.com/showhand-lab/flash-metrics-storage/utils     [no test files]
?       github.com/showhand-lab/flash-metrics-storage/utils/printer     [no test files]
```
