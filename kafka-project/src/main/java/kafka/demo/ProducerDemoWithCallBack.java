package kafka.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {
    public static void main(String[] args) {
        String bootstrapServers = "172.17.204.78:9092";
        //create producer properties
        Properties prop = new Properties();
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        for (int i = 0; i < 10; i++) {
            //create producer
            KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
            //create producer record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "hello world"+i);
            //send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("received metadata \n" +
                                "topic: " + recordMetadata.topic() + "\n" +
                                "partition: " + recordMetadata.partition() + "\n" +
                                "offset: " + recordMetadata.offset()
                        );
                    } else {
                        logger.error(e.getMessage());
                    }
                }
            });
            //flush data
            producer.flush();
            producer.close();
        }
    }
}
