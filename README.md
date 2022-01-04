# Flash Metrics Storage

```shell
$ cat << EOF > tidb.config
max-index-length = 12288
EOF

$ tiup playground nightly --db.config tidb.config

$ make test
Running test
ok      github.com/showhand-lab/flash-metrics-storage/metas     0.989s  coverage: 92.6% of statements
ok      github.com/showhand-lab/flash-metrics-storage/store     1.263s  coverage: 93.5% of statements
?       github.com/showhand-lab/flash-metrics-storage/table     [no test files]
?       github.com/showhand-lab/flash-metrics-storage/utils/printer     [no test files]
```