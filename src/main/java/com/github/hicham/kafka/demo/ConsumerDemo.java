package com.github.hicham.kafka.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        String bootstrapServers="172.17.107.6:9092";
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
        //create producer properties
        Properties prop=new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //create producer
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(prop);
        //create producer record
        ProducerRecord<String,String> record=new ProducerRecord<>("first_topic","hello world");
        //send data
        producer.send(record);
        //flush data
        producer.flush();
        producer.close();
    }
}
