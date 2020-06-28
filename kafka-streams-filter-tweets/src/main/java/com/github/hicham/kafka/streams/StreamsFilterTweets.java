package com.github.hicham.kafka.streams;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class StreamsFilterTweets {
    private static JsonParser jsonParser=new JsonParser();
    private static int extractUserFollowersInTweet(String tweetJson){
       try {
           return jsonParser.parse(tweetJson)
                   .getAsJsonObject()
                   .get("user")
                   .getAsJsonObject()
                   .get("followers_count")
                   .getAsInt();
       }catch(Exception e){
           return 0;
       }
    }
    public static void main(String[] args) {
        String bootstrapServers="192.168.141.248:9092";
        Logger logger = LoggerFactory.getLogger(StreamsFilterTweets.class);
        //create properties
        Properties prop = new Properties();
        prop.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        prop.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"demo-kafka-streams");
        prop.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        prop.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        //create topology
        StreamsBuilder streamsBuilder=new StreamsBuilder();
        //input topic
        KStream<String,String> inputTopic=streamsBuilder.stream("twitter_tweets");
        KStream<String,String> filteredStream=inputTopic.filter(
                (k,jsonTweet)->extractUserFollowersInTweet(jsonTweet)>1000
        );
        filteredStream.to("important_tweets");
        //build the topology
        KafkaStreams kafkaStreams=new KafkaStreams(streamsBuilder.build(),prop);
        //start our streams application
        kafkaStreams.start();
    }
}
