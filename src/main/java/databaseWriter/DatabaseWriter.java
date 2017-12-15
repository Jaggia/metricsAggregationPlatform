package databaseWriter;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.log4j.LogManager;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Pong;

import java.io.IOException;
import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class DatabaseWriter {

    private static final WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(Serdes.String().serializer());
    private static final WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer<>(Serdes.String().deserializer());
    private static final Serde<Windowed<String>> windowSerdes = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);
    private static final Serde<Long> longSerde = Serdes.Long();
    private static final Serde<String> stringSerde = Serdes.String();
    private static boolean INFLUX_ON = false;
    private static boolean ELASTIC_ON = false;

    private static InfluxDB influxDB;
    private static ArrayList<LinkedHashMap> agg;
    private static ArrayList<LinkedHashMap> global_tables;
    private static Map<String, Integer> timing;

    private static final org.apache.log4j.Logger logger = LogManager.getLogger(DatabaseWriter.class);

    private static String URL;
    private static String DB_NAME;
    private static String RP_NAME;
    private static String HOSTNAME;
    private static String INDEX;
    private static RestHighLevelClient highLevelClient;
    private static int PORT;
    private static int BATCH_SIZE;
    private static String APP_ID;
    private static String BOOTSTRAP_SERVERS;

    private enum whichDB {
        ELASTIC,
        INFLUX,
        ELASTIC_AND_INFLUX
    }

    /**
     * The constructor of the influx object
     * @param appConfig the configuration as per the YAML file
     */
    public DatabaseWriter(Map<String, Object> appConfig) {
        agg = (ArrayList<LinkedHashMap>) appConfig.get("agg");
        global_tables = (ArrayList<LinkedHashMap>) appConfig.get("global_tables");
        timing = (Map<String, Integer>) appConfig.get("interval");
        Map<String, Map<String, String>> kafka_inputs =
                (Map<String, Map<String, String>>) appConfig.get("kafka");
        BOOTSTRAP_SERVERS = kafka_inputs.get("input").get("bootstrap_servers");
        APP_ID = kafka_inputs.get("input").get("app_id");

        LinkedHashMap<String, String> influxConfig =
                (LinkedHashMap<String, String>) appConfig.get("influxdb");
        if (influxConfig != null) {
            INFLUX_ON = true;
            URL = influxConfig.get("connection");
            DB_NAME = influxConfig.get("db_name");
            RP_NAME = influxConfig.get("rp_name"); //for 2.7. can create only in >2.7
            try {
                influxDB = InfluxDBFactory.connect(URL, "root", "root");
                if (!influxDB.databaseExists(DB_NAME)) {
                    influxDB.createDatabase(DB_NAME);
                }
                influxDB.setDatabase(DB_NAME);
                influxDB.setRetentionPolicy(RP_NAME); //, dbName, "30d", "30m", 2, true);
            } catch (Exception e) {
                logger.info("Exception Name : " + e.getClass().getCanonicalName());
                logger.info("Exception Message : " + (e.getMessage()));
                logger.info("Exception StackTrace : ");
                e.printStackTrace();
            }
            if (!influxDB.isBatchEnabled()) {
                influxDB.enableBatch(2000, 100, TimeUnit.MILLISECONDS);
            }
            if (!influxDB.isGzipEnabled()) {
                influxDB.enableGzip();
            }
            logger.info("DatabaseWriter at : " + URL);
            //influxDB.setLogLevel(InfluxDB.LogLevel.HEADERS);
        }
        LinkedHashMap<String, Object> elasticConfig =
                (LinkedHashMap<String, Object>) appConfig.get("elastic");
        if (elasticConfig != null) {
            ELASTIC_ON = true;
            HOSTNAME = elasticConfig.get("hostname").toString();
            INDEX = elasticConfig.get("index").toString();
            PORT = Integer.parseInt(elasticConfig.get("port").toString());
            BATCH_SIZE = Integer.parseInt(elasticConfig.get("batchSize").toString());
            highLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(HOSTNAME, PORT, "http")));
        }
    }

    /**
     * The first function that is run in the application process.
     * This starts the stream after a call to createStreams()
     */
    void starter() {
        logger.info("Bootstrap Servers : " + BOOTSTRAP_SERVERS);
        logger.info("App ID: " + APP_ID);
        logger.info("Querying every : " + timing.get("event_count_interval") + " ms");
        Runtime runtime = Runtime.getRuntime();
        final NumberFormat format = NumberFormat.getInstance();
        final long maxMemory = runtime.maxMemory();
        final long allocatedMemory = runtime.totalMemory();
        final long freeMemory = runtime.freeMemory();
        final long mb = 1024 * 1024;
        final String mega = "MB";
        logger.info("========================== Memory Info ==========================");
        logger.info("Free memory: " + format.format(freeMemory / mb) + mega);
        logger.info("Allocated memory: " + format.format(allocatedMemory / mb) + mega);
        logger.info("Max memory: " + format.format(maxMemory / mb) + mega);
        logger.info("Total free memory: " + format.format((freeMemory + (maxMemory - allocatedMemory)) / mb) + mega);
        logger.info("*******************************");
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10 * 1024 * 1024L);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, System.getenv("NUM_THREADS"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        props.put(TopicConfig.CLEANUP_POLICY_COMPACT, "compact");
        props.put(TopicConfig.RETENTION_MS_CONFIG, "3600000"); // 1 HOUR
        props.put(TopicConfig.SEGMENT_MS_CONFIG, "300000");
        props.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.10");
        props.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, "7200000");
        props.put(TopicConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        final KafkaStreams streams = createStreams(props);
        final CountDownLatch latch = new CountDownLatch(1);

        try {
            streams.cleanUp();
            logger.info("Starting...");
            streams.start();
            streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
                if (!(throwable.getLocalizedMessage().startsWith("Assigned partition")
                    || (throwable.getLocalizedMessage().startsWith("Validation Failed")))) {
                    logger.info(throwable.getMessage());
                    throwable.printStackTrace();
                    System.exit(1);
                }
            });
            logger.info("Running :)");

            // attach shutdown handler to catch control-c
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Closing DatabaseWriter Stream...");
                logger.info("Restarting app. Will take 30 Seconds at max");
                influxDB.disableBatch();
                influxDB.close();
                streams.close(30, TimeUnit.SECONDS);
                latch.countDown();
                logger.info("DatabaseWriter Stream Closed :D");
            }));

            Timer timer = new Timer();

            if (System.getenv("SNAPSHOT").equals("0") || System.getenv("SNAPSHOT") == null) {
                timer.scheduleAtFixedRate(new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            //long timeTo = System.currentTimeMillis() - 600000L;
                            //long timeFrom = timeTo - 600000L;
                            long timeTo = System.currentTimeMillis();
                            long timeFrom = timeTo - timing.get("query_interval0_from").longValue();
                            //rounding
                            timeTo = (timeTo / 60000L) * 60000L;
                            timeFrom = (timeFrom / 60000L) * 60000L;
                            for (LinkedHashMap anAgg : agg) {
                                if ((INFLUX_ON && influxDB.ping() != null) || ELASTIC_ON) {
                                    queryTheSnapshot(streams,
                                            anAgg.get("name").toString(), //sum_b & count_pssc
                                            (ArrayList<String>) anAgg.get("group_by"), //globalTableNames to append in add2influx
                                            timeFrom, timeTo);
                                } else {
                                    throw new Exception("Not Connected to DatabaseWriter.");
                                }
                            }
                        } catch (Exception e) {
                            handleQueryException(e);
                        }
                    }
                }, 60000, timing.get("query_interval0").longValue());
            }

            if (System.getenv("SNAPSHOT").equals("1")) {
                    // influx chunk by 2 hours for 24 hour call
                long chunk = timing.get("chunk");
                long timeTo = System.currentTimeMillis();
                long timeFrom = timeTo - chunk;
                long endingTime = timeTo - timing.get("snapshot_time");
                //rounding
                timeTo = (timeTo / 60000L) * 60000L;
                timeFrom = (timeFrom / 60000L) * 60000L;
                for (LinkedHashMap anAgg : agg) {
                    try {
                        if ((INFLUX_ON && influxDB.ping() != null) || ELASTIC_ON) {
                            while (timeFrom >= endingTime - chunk) {
                                logger.info("CHUNKING from: " + new Date(timeFrom)
                                        + " to " + new Date(timeTo));
                                queryTheSnapshot(streams,
                                        anAgg.get("name").toString(), //sum_b_jag & count_pssc_jag
                                        (ArrayList<String>) anAgg.get("group_by"), //globalTableNames to append in add2influx
                                        timeFrom, timeTo);
                                timeTo = timeTo - chunk;
                                timeFrom = timeFrom - chunk;
                            }
                        } else {
                            if (INFLUX_ON && influxDB.ping() != null) {
                                throw new Exception("Not Connected to the Influx.");
                            }
                            throw new Exception("Not Connected to DB.");
                        }
                    } catch (Exception e) {
                        handleQueryException(e);
                    }
                    timeTo = System.currentTimeMillis();
                    timeFrom = timeTo - chunk;
                    endingTime = timeTo - timing.get("snapshot_time");
                }
                System.exit(0);
            }

            latch.await();
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
    }

    /**
     * Handles the exception during querying that is usually caused by losing
     * connection the influxDB
     * @param e the Exception
     */
    private void handleQueryException(Exception e) {
        if (!(ELASTIC_ON || INFLUX_ON)) {
            logger.info("None of the DBs are configured");
        } else if (!(INFLUX_ON && influxDB.ping() != null) && ELASTIC_ON) {
            logger.info("Influx DB cannot be reached");
        }
        logger.info("Exception Name : " + e.getClass().getCanonicalName());
        logger.info("Exception Message : " + (e.getMessage()));
        logger.info("Exception StackTrace : ");
        e.printStackTrace();
    }

    /**
     * Creates all streams and performs all the specified aggregations.
     * Also creates global tables.
     * @param streamsConfiguration the properties to configure the stream topology
     * @return an instance of KafkaStreams that we start() the application on
     */
    private static KafkaStreams createStreams(final Properties streamsConfiguration) {
        KStreamBuilder builder = new KStreamBuilder();
        //global table of phn2cachegroup etc
        for (LinkedHashMap global_table : global_tables) {
            String tableAndStoreName = global_table.get("name").toString();
            builder.globalTable(stringSerde, stringSerde, tableAndStoreName, tableAndStoreName);
        }
        //actual aggregations
        for (LinkedHashMap anAgg : agg) {
            if (anAgg.get("action").equals("mean")) {
                String tableTopicAndName = anAgg.get("name").toString();
                builder.globalTable(windowSerdes, Serdes.Double(), tableTopicAndName, tableTopicAndName);
            } else {
                String tableTopicAndName = anAgg.get("name").toString();
                builder.globalTable(windowSerdes, longSerde, tableTopicAndName, tableTopicAndName);
            }
        }
        return new KafkaStreams(builder, streamsConfiguration);
    }

    /**
     * Query a snapshot of the stream to send to InfluxDB for writing
     * @param streams the Kafka streams instance that contains the stores
     * @param storeName the name of the table to query, i.e - sumTable/countTable etc
     * @param globalTableNames the names of the global tables as per the configuration
     * @param timeFrom the time to query from
     * @param timeTo the time to query to
     * @throws InterruptedException if we Ctrl-C to end the flow of the application
     */
    private static void queryTheSnapshot(KafkaStreams streams,
                                         String storeName,
                                         ArrayList<String> globalTableNames,
                                         long timeFrom,
                                         long timeTo) throws InterruptedException {

        ReadOnlyKeyValueStore<Windowed<String>,Long> localAggStore
                = waitUntilStoreIsQueryable(storeName,
                QueryableStoreTypes.<Windowed<String>, Long>keyValueStore(),
                streams);
        logger.info("Got : " + storeName);
        logger.info("Querying from: " + new Date(timeFrom)
                + " to " + new Date(timeTo));
        KeyValueIterator<Windowed<String>, Long> keyValueIterator = localAggStore.all();
            add2DB(keyValueIterator,
                    streams, global_tables,
                    globalTableNames,
                    timeFrom,
                    timeTo);
        keyValueIterator.close();
    }

    /**
     * Adds all the results of kakfa streams query to the DatabaseWriter DB.
     * Also appends the request group_by info to each key. Retrieves what to extract from the global_table topic
     * @param iterator the iterator that contains the results of the query
     * @param streams the Kafka streams instance that contains the stores
     * @param global_tables the global table info from the configuration
     * @param globalTableNames the names of the global tables as per the configuration
     * @param timeFrom the time to query from
     * @param timeTo the time to query to
     */
    private static void add2DB(KeyValueIterator<Windowed<String>, Long> iterator,
                               KafkaStreams streams,
                               ArrayList<LinkedHashMap> global_tables,
                               ArrayList<String> globalTableNames,
                               long timeFrom,
                               long timeTo) {
        List<String> toWriteList = new ArrayList<>();
        BulkRequest bulkRequest = new BulkRequest();
        while (iterator.hasNext()) {
            KeyValue<Windowed<String>, Long> next = iterator.next();
            long recordTimestamp = next.key.window().start();
            // if record is withing query time range do the rest
            if (recordTimestamp <= timeTo && recordTimestamp >= timeFrom) {
                //manual join starts here
                StringBuilder newKey = new StringBuilder(next.key.key());
                for (int j = 0; j < globalTableNames.size(); j++) {
                    String globalTableName = globalTableNames.get(j);
                    ReadOnlyKeyValueStore view = streams.store(globalTableName, QueryableStoreTypes.keyValueStore());
                    String globalKey = global_tables.get(j).get("key").toString();
                    String globalValue = global_tables.get(j).get("value").toString();
                    int idx = newKey.indexOf(globalKey);
                    if (idx != -1) {
                        idx = idx + globalKey.length() + 1;
                        int idx2 = newKey.indexOf(",", idx);
                        if (idx2 != -1) {
                            String extraction = newKey.substring(idx, idx2);
                            String globalMatchedValue = view.get(extraction).toString();
                            newKey.append(globalValue).append("=")
                                    .append(globalMatchedValue).append(",");
                        }
                    }
                }
                if (INFLUX_ON) {
                    newKey.deleteCharAt(newKey.length() - 1);
                    String lineProtocolMessage =
                            newKey.toString() + " " +
                                    "value=" +
                                    next.value + " " +
                                    recordTimestamp * 1000000;
                    toWriteList.add(lineProtocolMessage);
                }
                if (ELASTIC_ON) {
                    String[] splits = newKey.toString().split("[,=]");
                    Map<String, Object> jsonMap = new HashMap<>();
                    jsonMap.put("measurement", splits[0]);
                    for (int i = 1; i < splits.length; i += 2) {
                        jsonMap.put(splits[i], splits[i + 1]);
                    }
                    jsonMap.put("value", next.value);
                    jsonMap.put("@timestamp", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").format(new Date(recordTimestamp)));
                    String hex = (newKey.toString() + recordTimestamp);
                    byte[] hexBytes = hex.getBytes(Charset.availableCharsets().get("UTF-8"));
                    UpdateRequest request = new UpdateRequest(
                            INDEX,
                            "doc",
                            DigestUtils.sha1Hex(hexBytes));
                    bulkRequest.add(request.doc(jsonMap).upsert(jsonMap));
                    if (ELASTIC_ON && bulkRequest.numberOfActions() % BATCH_SIZE == 0) {
                        write2Elastic(bulkRequest);
                        bulkRequest = new BulkRequest();
                    }
                }
            }
        }
        if (INFLUX_ON) {
            influxDB.write(toWriteList);
            logger.info("(InfluxDB) Wrote : " + toWriteList.size() + " records");
            if (toWriteList.size() != 0) {
                logger.info(toWriteList.get(toWriteList.size() - 1));
            }
        }
        if (ELASTIC_ON) {
            write2Elastic(bulkRequest);
        }
    }

    /**
     * Function to write to elastic search. Takes in a max of 500 records at once
     * @param bulkRequest the bulk request to write to ElasticSearch
     */
    private static void write2Elastic(BulkRequest bulkRequest) {
        try {
            if (bulkRequest.numberOfActions() != 0) {
                BulkResponse response = highLevelClient.bulk(bulkRequest);
                logger.info("(ElasticSearch) Wrote " + response.getItems().length + " records.");
            } else {
                logger.info("(ElasticSearch) Wrote 0 records.");
            }
        } catch (IOException e) {
            logger.info("Exception Name : " + e.getClass().getCanonicalName());
            logger.info("Exception Message : " + (e.getMessage()));
            logger.info("Exception StackTrace : ");
            e.printStackTrace();
        }
    }


    /**
     * Whenever we query the application, there's a chance in the initial moments that Kafka
     * is still repartioning the data so the store may be temporarily unavailable. This function just waits
     * until that is done and returns it to the query function
     * @param storeName the store name we're querying
     * @param queryableStoreType the type of query we want. i.e - Readonly
     * @param streams the Kafka streams instance that contains the stores
     * @param <T> Generic type to return. Can be any type of store query result
     * @return the state store that we want to query
     * @throws InterruptedException if we Ctrl-C to end the flow of the application
     */
    // Wait until the store of type T is queryable. When it is, return a reference to the store.
    private static <T> T waitUntilStoreIsQueryable(final String storeName,
                                                    final QueryableStoreType<T> queryableStoreType,
                                                   final KafkaStreams streams) throws InterruptedException {
        while (true) {
            try {
                return streams.store(storeName, queryableStoreType);
            } catch (InvalidStateStoreException ignored) {
                // store not yet ready for querying
                Thread.sleep(5000);
                logger.info("Fetching state stores... " + storeName);
            }
        }
    }

    /**
     * Writes the number of messages so far, since the app started, to influx.
     * @param count the number of messages since this instance of the app started
     * @param hostname the container hostname for grouping in influxDB
     */
    public void writeMessageCount(long count, String hostname) {
        influxDB.write("messageCount,hostname="  + hostname + " value=" + count);
    }

    /**
     *  Pings the influxDB connection
     *  @return the Pong object. (null if not connected)
     */
    public Pong getPing() {
        return influxDB.ping();
    }
}
