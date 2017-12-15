package metricsAggregationPlatform;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import databaseWriter.DatabaseWriter;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.log4j.LogManager;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


class Consumer {
    private static String BOOTSTRAP_SERVERS;
    private static String INPUT_TOPIC;
    private static String OUTPUT_TOPIC;
    private static String APP_ID;
    private static String CONTAINER_ID;
    private static ArrayList<LinkedHashMap> agg;
    private static ArrayList<LinkedHashMap> global_tables;
    private static Map<String, Integer> timing;
    private static JsonParser parser;
    private static JsonObject jsonObject;
    private static DatabaseWriter DatabaseWriter;
    private static Aggregator Aggregator;

    private static final org.apache.log4j.Logger logger = LogManager.getLogger(Consumer.class);

    private static final WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(Serdes.String().serializer());
    private static final WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer<>(Serdes.String().deserializer());
    private static final Serde<Windowed<String>> windowSerdes = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);
    private static final Serde<String> stringSerde = Serdes.String();
    private static final Serde<Long> longSerde = Serdes.Long();

    /**
     * Constructor for the consumer class. Takes in the YAML config and sets
     * all local variables for future use.
     * @param configuration the Map<String, Object> for the configuration
     */
    Consumer(Map<String, Object> configuration) {
        agg = (ArrayList<LinkedHashMap>) configuration.get("agg");
        global_tables = (ArrayList<LinkedHashMap>) configuration.get("global_tables");
        Map<String, Map<String, String>> kafka_inputs =
                (Map<String, Map<String, String>>) configuration.get("kafka");
        BOOTSTRAP_SERVERS = kafka_inputs.get("input").get("bootstrap_servers");
        INPUT_TOPIC = kafka_inputs.get("input").get("input_topic");
        OUTPUT_TOPIC = kafka_inputs.get("output").get("output_topic");
        APP_ID = kafka_inputs.get("input").get("app_id");
        CONTAINER_ID = System.getenv("HOSTNAME");
        timing = (Map<String, Integer>) configuration.get("interval");;
        parser = new JsonParser();
        jsonObject = new JsonObject();
        DatabaseWriter = new DatabaseWriter(configuration);
        Aggregator = new Aggregator();
    }

    /**
     * The first function that is run in the application process.
     * This starts the stream after a call to createStreams()
     */
    void starter() {
        logger.info("Bootstrap Servers : " + BOOTSTRAP_SERVERS);
        logger.info("Input Topic : " + INPUT_TOPIC);
        logger.info("Output Topic : " + OUTPUT_TOPIC);
        logger.info("App ID: " + APP_ID);
        logger.info("Event Counter every : " + timing.get("event_count_interval") + " ms");
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
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TimeStampExtractor.class);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, System.getenv("NUM_THREADS"));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");

        props.put(TopicConfig.CLEANUP_POLICY_COMPACT, "compact");
        props.put(TopicConfig.RETENTION_MS_CONFIG, "3600000"); // 1 HOUR
        props.put(TopicConfig.SEGMENT_MS_CONFIG, "300000");
        props.put(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.10");
        props.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, "7200000");
        props.put(TopicConfig.COMPRESSION_TYPE_CONFIG, "gzip");


        final AtomicLong counter = new AtomicLong();
        final KafkaStreams streams = createStreams(props, counter);
        final CountDownLatch latch = new CountDownLatch(1);

        try {
            streams.cleanUp();
            logger.info("Starting...");
            streams.start();
            streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
                logger.info(throwable.getMessage());
                throwable.printStackTrace();
                System.exit(1);
            });
            logger.info("Running :)");

            // attach shutdown handler to catch control-c
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Closing Consumer Stream...");
                logger.info("Restarting app. Will take 30 Seconds at max");
                streams.close(30, TimeUnit.SECONDS);
                latch.countDown();
                logger.info("Consumer Stream Closed :D");
            }));

            AtomicLong previousCount = new AtomicLong(-1);
            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    Boolean halted = false;
                    if (streams.state() == KafkaStreams.State.RUNNING) {
                        try {
                            Long tempValueHold = counter.longValue();
                            if (DatabaseWriter != null && DatabaseWriter.getPing() != null) {
                                if (!counterHalted(tempValueHold, previousCount)) {
                                    DatabaseWriter.writeMessageCount(tempValueHold, CONTAINER_ID);
                                    previousCount.set(tempValueHold);
                                } else {
                                    halted = true;
                                    logger.info("Messages so far : " + counter.longValue());
                                    throw new Exception("Message counter halted");
                                }
                            } else {
                                throw new Exception("Not connected to Influx");
                            }
                            logger.info("Messages so far : " + counter.longValue());
                        } catch (Exception e) {
                            logger.info("Exception Name : " + e.getClass().getCanonicalName());
                            logger.info("Exception Message : " + (e.getMessage()));
                            logger.info("Exception StackTrace : ");
                            e.printStackTrace();
                            if (halted) {
                                System.exit(1);
                            }
                        }
                    }
                }
            }, 10000, timing.get("event_count_interval").longValue());

            latch.await();
        } catch (Throwable e) {
           e.printStackTrace();
        }
    } // End Starter

    /**
     * Checks to see if the message count has stopped incrementing.
     * If stopped, the app will restart. If not, then we keep going.
     * @param counter the current counter keeping track of messages so far
     * @param previousCount the counter that holds the previous value so we can compare
     * @return true if both are same, false if not
     */
     private boolean counterHalted(Long counter, AtomicLong previousCount) {
         return counter != 0 && previousCount.get() != 0 && counter == previousCount.get();
     }

    /**
     * Creates all streams and performs all the specified aggregations.
     * Also creates global tables.
     * @param streamsConfiguration the properties to configure the stream topology
     * @param counter the atomic counter for seeing the number of messages per second
     * @return an instance of KafkaStreams that we start() the application on
     */
    private static KafkaStreams createStreams(final Properties streamsConfiguration, AtomicLong counter) {

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> inputStream = builder.stream(stringSerde, stringSerde, INPUT_TOPIC);
        createAllGlobalTables(inputStream);

        KStream<String, String> outputStream = inputStream.flatMap((key, value) -> {
            List<KeyValue<String, String>> result = new ArrayList<>();
            counter.incrementAndGet();
            for (LinkedHashMap anAgg : agg) {
                try {
                    List<String> tags = (ArrayList<String>) anAgg.get("tags");
                    String AGG_NAME = anAgg.get("name").toString();
                    jsonObject = parser.parse(value).getAsJsonObject();
                    StringBuilder newKey = new StringBuilder(AGG_NAME + ",");
                    for (String myKey : tags) {
                        newKey.append(myKey).append("=").append(jsonObject.get(myKey).getAsString()).append(",");
                    }
                    result.add(new KeyValue<>(newKey.toString(), value));
                } catch (Exception e) {
                    logger.info("Exception Name : " + e.getClass().getCanonicalName());
                    logger.info("Exception Message : " + (e.getMessage()));
                    logger.info("(FlatMap)Culprit Value : " + value);
                    logger.info("Exception StackTrace : ");
                    e.printStackTrace();
                }
            }
            return result;
        });

        for (LinkedHashMap anAgg : agg) {
            String MAIN_ACTION = (String) anAgg.get("action");
            String MAIN_ACTION_FIELD = (String) anAgg.get("action_field");
            String AGG_NAME = anAgg.get("name").toString();
            try {
                switch (MAIN_ACTION) {
                    case "count":
                        sendToOutputTopic(Aggregator.count(outputStream, AGG_NAME), AGG_NAME, longSerde);
                        break;
                    case "sum":
                        sendToOutputTopic(Aggregator.sum(outputStream, AGG_NAME, MAIN_ACTION_FIELD), AGG_NAME, longSerde);
                        break;
                    case "mean":
                        sendToOutputTopic(Aggregator.mean(outputStream, AGG_NAME, MAIN_ACTION_FIELD), AGG_NAME, Serdes.Double());
                        break;
                }
                //SEND TO OUTPUT TOPIC
            } catch (Exception e) {
                logger.info("Exception Name : " + e.getClass().getCanonicalName());
                logger.info("Exception Message : " + (e.getMessage()));
                logger.info("Exception Occured During Aggregation Function. Most likely due to " +
                        "a JSON parsing error.");
                logger.info("Exception StackTrace : ");
                e.printStackTrace();
            }
        }
        return new KafkaStreams(builder, streamsConfiguration);
    }

    /**
     * Sends aggregated table to the specified output topic
     * @param kTable the aggregated and grouped kTable that is to be sent
     *               to the {@param outputTopic}
     * @param aggName the final output topic to send the agged stream to
     * @param valSerde the type of serdes to use depending on the type of key of the kTable
     */
    private static <T> void sendToOutputTopic(KTable<Windowed<String>, T> kTable,
                                              String aggName, Serde<T> valSerde) {
        logger.info("Sending to output topic : " + aggName);
        kTable.toStream().to(windowSerdes, valSerde, aggName);
        }

    /**
     * Creates all required global tables for group_by style aggregations
     * @param inputStream the initial raw input kafka stream
     */
    private static void createAllGlobalTables(KStream<String, String> inputStream)
                                                        throws IllegalStateException{
        for (LinkedHashMap global_table : global_tables) {
            String configKey = global_table.get("key").toString();
            String configValue = global_table.get("value").toString();
            KStream<String, String> globalTableCreator = inputStream.map((key, value) -> {
                try {
                    JsonObject obj = parser.parse(value).getAsJsonObject();
                    return new KeyValue<>(obj.get(configKey).getAsString(), obj.get(configValue).getAsString());
                } catch (Exception e) {
                    logger.info("Exception Name : " + e.getClass().getCanonicalName());
                    logger.info("Exception Message : " + (e.getMessage()));
                    logger.info("(GlobalTable)Culprit Value : " + value);
                    logger.info("Exception StackTrace : ");
                    e.printStackTrace();
                    return new KeyValue<>("cow", "say");
                }
            });

            String topicAndName = global_table.get("name").toString();
            globalTableCreator.to(topicAndName);
        }
    }
}
