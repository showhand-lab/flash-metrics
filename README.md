# Flash Metrics Storage

bootstrap:
```shell
$ echo -e "max-index-length = 12288" > tidb.config

$ tiup playground nightly --db.config tidb.config

$ make 

$ ./bin/flash-metrics-storage --config.file=./flashmetrics.yml.example 
```

Then, configure your grafana, add a new "prometheus" datasource with host `127.0.0.1:9977`.
