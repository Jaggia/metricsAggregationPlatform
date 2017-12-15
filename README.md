# metricsAggregationPlatform

This app aggregates all your kafka logs and ships the results to influxDB.
The points are then visualized using grafana.

As long as your logs ship from Kafka in JSON format, this app will aggregate it :)

Create an output topic as well (to match the output topic from the configuration settings below)
The topic name has to be the same as all the name fields in the YAML config file. In the example below,
there will be 4 topics named phn_to_cachegroup,cachegroup_to_cdn, count_pssc, and sum_b.
```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 16 --config retention.ms=3600000 --config cleanup.policy=compact --config segment.ms=300000 --topic TOPICNAME
```

cd into the metricsAggregationPlatform directory and run the following commands to initialize the needed
docker containers in this order.

```
docker-compose up -d logstash kafka grafana kibana
docker-compose up --scale kafka_jar=4

```
You can scale the app as needed by putting in whatever number you'd like.


Sample YAML config file.
```
kafka:
  input:
    bootstrap_servers: kafka01
    input_topic: myKafkaTopic
    app_id: myID
  output:
    output_topic: output

influxdb:
  connection : myInfluxLink
  db_name : Database
  rp_name : autogen

elastic:
  hostname : elasticHost
  scheme : http
  port : 9200
  index : snafu
  batchSize : 100000

interval:
  query_interval0: 60000 # 60 * 1000 - one minute
  query_interval0_from: 600000 # 10 * 60 * 1000 - ten minutes
  chunk: 3600000 # 60 * 60 * 1000 - one hour
  snapshot_time: 86400000 # 24 * 60 * 60 * 1000 - twenty-four hours
  event_count_interval: 10000 # 10 * 1000 - ten seconds

global_tables:
  -
   name: phn_to_cachegroup
   key: phn
   value: cachegroup
  -
   name: cachegroup_to_cdn
   key: cachegroup
   value: cdn

agg:

  -
   name: count_pssc
   action: count
   action_field: pssc
   tags:
     - pssc
     - phn
     - shn
   group_by:
     - phn_to_cachegroup
     - cachegroup_to_cdn

  -
   name: count_crc
   action: count
   action_field: crc
   tags:
     - crc
     - phn
     - shn
   group_by:
     - phn_to_cachegroup
     - cachegroup_to_cdn
  -
   name: sum_b
   action: sum
   action_field: b
   tags:
     - phn
     - shn
   group_by:
     - phn_to_cachegroup
     - cachegroup_to_cdn
```
A guide to this configuration :

```
kafka:
    input: All the kafka connection configs
    output: All the kafka connection configs

influxdb: All the kafka connection configs
elastic: All the elastic connection configs

interval:
    At specified periods of time(query_interval0 and query_interval1), the app will query the logs.
    The time range it queries is from now to (now - the number of milliseconds as per query_interval0_from and query_interval1_from)
    if running the snapshot_jar, the 'chunk' and 'snapshot_time' fields are important. If for example, you'd like to query
    the last 24 hours, snapshot_time would be 86400000. The chunk field determines how to divide the snapshot_time. if you say
    chunk is 3600000, then the snapshot of the past 24 hours (86400000 ms), would be queried in chunks of
    1 hour (3600000 ms).
    event_count_interval : the period of how often we send the number of messages since the app started to influxDB
  In the example above, every one minute (query_interval0) the app will query logs from 10 minutes ago (query_interval0_from).

global_tables:
    name: the name of this aggregation and the name of the measurement in influxDB. Also serves
            as the topic name for kafka when you create them as per the instructions above.
    key : the key of the global table. this will be taken from the JSON log
    value :  the value of the global table. this will be taken from the JSON log

agg:
  name: the name of this aggregation and the name of the measurement in influxDB. Also serves
  as the topic name for kafka when you create them as per the instructions above.
  action: either sum or count.
  action_field: the JSON key to perform the aggregation on. eg - pssc or b (bytes)
  tags : the JSON keys you want to filter from the raw JSON log (must include action field as well)
  group_by : the names of the global_tables above. This is appended to the results of the filtering of the tags above.
```

All the above configs are required except for influxdb and elastic configs.
You can have either one, both or none. The choice of the DB to write to is up to you.
