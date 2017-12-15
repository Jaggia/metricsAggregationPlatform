package metricsAggregationPlatform;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;

class Aggregator {

    /**
     * Count aggregation function
     * @param kStream The kStream to perform the aggregation upon
     * @param AGG_NAME the name field under agg in the YAML file.
     *                 This is also the topic name & the string that differentiates the keys
     * @return the final aggregated kTable
     */
    KTable<Windowed<String>, Long> count(KStream<String, String> kStream,
                                                        String AGG_NAME) {
        String starterCheck = AGG_NAME.split("-")[0];
        //Group the Stream based on the key
        KGroupedStream<String, String> kGroupedStreamCount
                = kStream.groupBy((key, value) -> key.startsWith(starterCheck) ? key : null);
        //END COUNT OPERATIONS
        return kGroupedStreamCount.aggregate(
                () -> 0L,
                (aggKey, newValue, aggValue) -> aggValue + 1,
                TimeWindows.of(60 * 1000L).until(24 * 60 * 60 * 1000L), //1 min windows for 24 hours
                Serdes.Long(),
                AGG_NAME);
    }

    /**
     * Sum aggregation function
     * @param kStream The kStream to perform the aggregation upon
     * @param AGG_NAME the name field under agg in the YAML file.
     *                 This is also the topic name & the string that differentiates the keys
     * @param MAIN_ACTION_FIELD the field which we want to add up
     * @return the final aggregated kTable
     */
    KTable<Windowed<String>, Long> sum(KStream<String, String> kStream,
                                                      String AGG_NAME,
                                                      String MAIN_ACTION_FIELD) {
        String starterCheck = AGG_NAME.split("-")[0];
        //Group the Stream based on the key
        KGroupedStream<String, String> kGroupedStreamSum
                = kStream.groupBy((key, value) -> key.startsWith(starterCheck) ? key : null);
        //END SUM OPERATIONS
        return kGroupedStreamSum.aggregate(
                () -> 0L,
                (aggKey, newValue, aggValue) -> {
                    JsonObject jsonObject = new JsonParser().parse(newValue).getAsJsonObject();
                    return aggValue + jsonObject.get(MAIN_ACTION_FIELD).getAsLong();
                },
                TimeWindows.of(60 * 1000L).until(24 * 60 * 60 * 1000L), //1 min windows for 24 hours
                Serdes.Long(),
                AGG_NAME);
    }

    /**
     * Average aggregation function
     * @param kStream The kStream to perform the aggregation upon
     * @param AGG_NAME the name field under agg in the YAML file.
     *                 This is also the topic name & the string that differentiates the keys
     * @param MAIN_ACTION_FIELD the field which we want to average up
     * @return the final aggregated kTable
     */
    KTable<Windowed<String>, Double> mean(KStream<String, String> kStream,
                                          String AGG_NAME,
                                          String MAIN_ACTION_FIELD) {
        KTable<Windowed<String>, Long> sumTable
                = sum(kStream, AGG_NAME+"-sum", MAIN_ACTION_FIELD);
        KTable<Windowed<String>, Long> countTable
                = count(kStream, AGG_NAME+"-count");
        return sumTable.leftJoin(countTable,
                (Long sum, Long count) -> ((sum != null) && (count != null)) ? (sum.doubleValue() / count.doubleValue()) : null);
    }

}
