package learn.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerWithThreads {
    static Properties properties = new Properties();

    public static void main(String[] args) {

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "java-app-2");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        var latch = new CountDownLatch(1);
        var runnable = new ConsumerThread(latch, "new_topic15");
        var thread = new Thread(runnable);
        thread.start();

        try {
            latch.await();
        } catch (InterruptedException e) {
            System.out.println("App is interupted");
        } finally {
            System.out.println("App is closing");
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("caught shutdown hook");
            runnable.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));
    }

    public static class ConsumerThread implements Runnable {

        private final CountDownLatch latch;
        KafkaConsumer<String, String> consumer;

        public ConsumerThread(CountDownLatch latch, String topic) {
            this.latch = latch;
            consumer = new KafkaConsumer<>(properties);
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    System.out.println("-");
                    var records = consumer.poll(Duration.ofMillis(1000));
                    records.forEach(System.out::println);
                }
            } catch (WakeupException e) {
                System.out.println("Received shutdown signal!");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            //interupt consumer.poll();
            //will throw wakeupException
            consumer.wakeup();
        }
    }
}
