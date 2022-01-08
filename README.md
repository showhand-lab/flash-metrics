# Flash Metrics Storage

```shell
$ cat << EOF > tidb.config
max-index-length = 12288
EOF

$ tiup playground nightly --db.config tidb.config

$ make test
Running test
ok      github.com/showhand-lab/flash-metrics/metas     1.157s  coverage: 94.3% of statements
ok      github.com/showhand-lab/flash-metrics/remote    2.419s  coverage: 72.4% of statements
?       github.com/showhand-lab/flash-metrics/service   [no test files]
?       github.com/showhand-lab/flash-metrics/service/http      [no test files]
ok      github.com/showhand-lab/flash-metrics/store     1.640s  coverage: 93.5% of statements
?       github.com/showhand-lab/flash-metrics/table     [no test files]
?       github.com/showhand-lab/flash-metrics/utils     [no test files]
?       github.com/showhand-lab/flash-metrics/utils/printer     [no test files]
```
