package learn.kafka.basics;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {
        var producer = createKafkaProducer();
        System.out.println("producer");
        for (int i = 0; i < 15; i++) {


            producer.send(new ProducerRecord<>("montag_topic", "k" + i, "m" + i), (metadata, exception) -> {
                if (metadata != null) {
                    System.out.println("+----------------------------------+");
                    System.out.println("topic " + metadata.topic());
                    System.out.println("partition " + metadata.partition());
                    System.out.println("offset " + metadata.offset());
                    System.out.println("timestamp " + metadata.timestamp());
                    System.out.println("+----------------------------------+");
                } else if (exception != null) {
                    exception.printStackTrace();
                }
            });

        }
        producer.flush();
        producer.close();
        System.out.println("finished");

    }

    static public KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(properties);
    }
}
