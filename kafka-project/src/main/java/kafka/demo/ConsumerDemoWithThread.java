package kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class ConsumerDemoWithThread {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        new ConsumerDemoWithThread().run();
    }
    private void run()   {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
        String bootstrapServers = "172.17.204.78:9092";
        String groupId="my_fourth_application";
        String topic="first_topic";
        CountDownLatch latch=new CountDownLatch(1);
        logger.info("creating myConsumerThread");
        Runnable myConsumerRunnable=new ConsumerRunnable(latch,bootstrapServers,groupId,topic);
        Thread myThread=new Thread(myConsumerRunnable);
        myThread.start();
        //add a  shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
          logger.info("caught a shutdown hook");
            ((ConsumerRunnable) myConsumerRunnable).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("application interrupted",e);
            }
            finally {
                logger.error("application closing");
            }
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("application interrupted",e);
        }
        finally {
            logger.error("application closing");
        }
    }

    public class ConsumerRunnable implements Runnable{
        private CountDownLatch latch;
        private Logger logger;
        private KafkaConsumer<String,String> consumer;
       public ConsumerRunnable(CountDownLatch latch,
                             String bootstrapServers,
                             String groupId,
                             String topic)
       {
           this.latch=latch;
           //create consmer properties
           Properties prop = new Properties();
           Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);
           prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
           prop.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
           prop.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
           prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
           prop.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

           //create consumer
           consumer=new KafkaConsumer<String, String>(prop);
           consumer.subscribe(Arrays.asList(topic));
       }
        @Override
        public void run() {
           try {
               while (true) {
                   ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                   for (ConsumerRecord record : records) {
                       logger.info("topic: " + record.topic() + " partition: " + record.partition() + " offset: " + record.offset() + " key: " + record.key() + " value: " + record.value());
                   }
               }
           }
           catch(WakeupException e){logger.info("received shutdown signal!");}
           finally{consumer.close();latch.countDown();}
        }
        public void shutdown(){
            consumer.wakeup();
        }
    }
}
