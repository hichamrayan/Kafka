package kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ConsumerDemoGroups {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServers = "172.17.204.78:9092";
        String groupId="my_fifth_application";
        String topic="first_topic";
        //create consmer properties
        Properties prop = new Properties();
        Logger logger = LoggerFactory.getLogger(ConsumerDemoGroups.class);
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //create consumer
        KafkaConsumer<String,String> consumer=new KafkaConsumer<String, String>(prop);
        consumer.subscribe(Arrays.asList(topic));
        while(true){
            ConsumerRecords<String,String> records=consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord record: records){
                logger.info("topic: "+record.topic()+" partition: "+record.partition()+" offset: "+record.offset()+" key: "+record.key()+" value: "+record.value());
            }
        }
    }
}
