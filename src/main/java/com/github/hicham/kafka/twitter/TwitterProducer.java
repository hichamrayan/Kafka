package com.github.hicham.kafka.twitter;

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
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
<<<<<<< HEAD
    List<String> terms = Lists.newArrayList("bitcoin");
=======
    List<String> terms = Lists.newArrayList("bitcoin","usa","canada","sport");
>>>>>>> f02aaf9... Kafka+Twitter API
    String consumerKey="LxzEJXD0dvqBOGf4oVdSX6MkD";
    String consumerSecret="ENxpug1a1a3cGZ7WQM1ba60EutqOhrRQSZZvmRnysXkRqPXrgA";
    String token="1274875904913633281-HG6CC5HhqzMYLlMgJ8U89SG4wnn8oe";
    String secret="AVxk5mBm8Hkm508O1tArB4G9qBxkz9TSAIWh85lZWDTQi";
    Logger logger;

    public TwitterProducer() {
        logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    }

    private void run(){
        logger.info("start application!");
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);
        //create a twitter client
        Client client=createTwitterClient(msgQueue);
        // Attempts to establish a connection.
        client.connect();
        KafkaProducer<String,String> producer=createKafkaProducer();
        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            logger.info("Stopping application");
            logger.info("Stopping client");
            client.stop();
            logger.info("closing producer");
            producer.close();
            logger.info("done!");
        }));

        //loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
                client.stop();
            }
            if(msg!=null) producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e!=null)logger.error("error sneding msg",e);
                }
            });
        }
        logger.info("Application end!");
    }
    private Client createTwitterClient(BlockingQueue<String> msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);
  //Creating a client:
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }
    private KafkaProducer<String,String> createKafkaProducer(){
        String bootstrapServers="172.17.204.78:9092";
        //create producer properties
        Properties prop=new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create a safe producer
        prop.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        prop.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        prop.setProperty(ProducerConfig.RETRIES_CONFIG,""+Integer.MAX_VALUE);
        prop.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");
<<<<<<< HEAD
=======
        //high throughput producer (at the expense of latency and cpu usage)
        prop.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        prop.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        prop.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,""+(32*1024));
>>>>>>> f02aaf9... Kafka+Twitter API
        //create producer
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(prop);
        return producer;
    }

    public static void main(String[] args) {
       new TwitterProducer().run();
    }
}
