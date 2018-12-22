package com.github.kafka.course;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerApplicationWithThreads {
    private static Logger logger = LoggerFactory.getLogger(ConsumerApplicationWithThreads.class);

    public static void main(String[] args) {
           String bootstrapServer = "localhost:9092";
           String topic = "com.mytopic.metric";
           String groupId = "myapp-first";

        CountDownLatch latch = new CountDownLatch(1);

           Runnable consumerThread = new ConsumerThread(
                   latch,
                   topic,
                   bootstrapServer,
                   groupId
           );
           Thread thread = new Thread(consumerThread);
           thread.start();

           // shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(
                    ()-> {
                        logger.info("caught shutdown hook");
                        ((ConsumerThread) consumerThread).shutdown();
                        try{
                            latch.await();
                        }catch (InterruptedException e){
                           e.printStackTrace();
                        }
                    }
            ));

           try {
               latch.wait();
           }catch (InterruptedException ex){
               ex.printStackTrace();
           }finally {
               logger.info("Applicaiton Consumer closed.");
           }
    }

}
