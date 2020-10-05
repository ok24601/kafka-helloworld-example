package learn.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "java-app");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singleton("twitter_topic_tweets"));

        while (true) {
            System.out.println("-");
            var records = consumer.poll(Duration.ofMillis(1000));
            records.forEach(System.out::println);
        }
    }
}
