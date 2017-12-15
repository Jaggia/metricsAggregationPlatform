package metricsAggregationPlatform;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

// Extracts the embedded timestamp of a record (giving you "event-time" semantics).
public class TimeStampExtractor implements TimestampExtractor {

    @Override
    public long extract(final ConsumerRecord<Object, Object> record, final long previousTimestamp) {
        long timestamp = -1;
        try {
            JsonParser parser = new JsonParser();
            JsonElement element = parser.parse(record.value().toString());
            JsonObject obj = element.getAsJsonObject(); //since you know it's a JsonObject
            if (obj.get("cqtq") != null) {
                timestamp = obj.get("cqtq").getAsLong();
            } else {
                String date = obj.get("@timestamp").getAsString();
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.US);
                format.setTimeZone(TimeZone.getTimeZone("UTC"));
                Date date1 = format.parse(date);
                timestamp = date1.getTime();
            }
        } catch (Exception e) {
            System.out.println("Exception Name : " + e.getClass().getCanonicalName());
            System.out.println("Exception Message : " + (e.getMessage()));
            System.out.println("Culprit Value : " + record.value());
            System.out.println("Exception StackTrace : ");
            e.printStackTrace();
        }
        return timestamp;
    }
}