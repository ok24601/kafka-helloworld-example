package learn.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import learn.kafka.basics.ProducerDemo;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    KafkaProducer<String, String> producer = ProducerDemo.createKafkaProducer();

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run()  {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        var client = createTwitterClient(msgQueue);
        client.connect();

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
                System.out.println("put message into kafka: " + msg);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                producer.send(new ProducerRecord<>("twitter_topic", null, msg));
                System.out.println("----------");
            }

        }
        System.out.println("end of application");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {


        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("bitcoin");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1("KpcIQdz9DVicBTz6jrVXXyENs",
                "1G2CvtzBMXzJSlLrWbN04cBVBpB3XC4HJ0BWB9XzIH7rtzN4Qs",
                "1298574836885852160-LDKjNBu7il99e1gZSoAfQ1oqvJLzNb",
                "lDYen1nxrUPJJHC04MyhtuApI3GtNOE2OgfChnhqGvkea");

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;

    }
}
