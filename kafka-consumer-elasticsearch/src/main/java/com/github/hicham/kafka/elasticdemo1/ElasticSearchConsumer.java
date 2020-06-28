package com.github.hicham.kafka.elasticdemo1;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer{
    // don't do that if you run a local ES
        public static RestHighLevelClient createClient(){
            //https://wbv8ctkfgy:lsu39bn9vw@kafka-project-2025477351.us-west-2.bonsaisearch.net:443
            String hostname="kafka-project-2025477351.us-west-2.bonsaisearch.net";
            String username="wbv8ctkfgy";
            String password="lsu39bn9vw";
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
            RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443,"https"))
                    .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                        @Override
                        public HttpAsyncClientBuilder customizeHttpClient(
                                HttpAsyncClientBuilder httpClientBuilder) {
                            return httpClientBuilder
                                    .setDefaultCredentialsProvider(credentialsProvider);
                        }
                    });
            RestHighLevelClient client=new RestHighLevelClient(builder);
            return client;
    }
    public static KafkaConsumer<String,String> createConsumer(String topic){
        String bootstrapServers = "172.17.204.78:9092";
        String groupId="kafka-demo-elasticsearch";
        //create consmer properties
        Properties prop = new Properties();
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        prop.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        prop.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        //create consumer
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(prop);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }
   private static JsonParser jsonParser=new JsonParser();
    private static String extractIdFromTweet(String tweetJson){

        return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();
        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            logger.info("received records: "+records.count());
            for (ConsumerRecord record : records) {
                logger.info(record.value().toString());
                //String id=record.topic()+"_"+record.partition()+"_"+record.offset();
                String id=extractIdFromTweet(record.value().toString());
                IndexRequest indexRequest = new IndexRequest("twitter","tweets",id)
                .source(record.value(), XContentType.JSON);
                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                logger.info(indexResponse.getId());
            }
            logger.info("commiting data");
            consumer.commitSync();
            logger.info("data commited");
            Thread.sleep(1000);
        }

    }
}
