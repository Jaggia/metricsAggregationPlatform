package metricsAggregationPlatform;

import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class ConsumerRunner {
    /**
     * The main ConsumerRunner class.
     */
    public static void main(String[] args) {
        Map<String, Object> appConfig;
        Yaml yaml = new Yaml();

        // try (InputStream in = new FileInputStream("/etc/jaggia/ymlConfigKafka.yml")) {
        try (InputStream in = new FileInputStream("/etc/jaggia/ymlConfig.yml")) {
            appConfig = (Map<String, Object>) yaml.load(in);
            Consumer consumer= new Consumer(appConfig);
            consumer.starter();
        }
        catch (IOException e) {
            System.out.println("Exception Name : " + e.getClass().getCanonicalName());
            System.out.println("Exception Message : " + (e.getMessage()));
            System.out.println("Exception StackTrace : ");
            e.printStackTrace();
        }
    }
}
