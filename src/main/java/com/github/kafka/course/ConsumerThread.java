package com.github.kafka.course;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerThread implements Runnable {
    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;
    private Logger logger = LoggerFactory.getLogger(ConsumerThread.class);


    public ConsumerThread(CountDownLatch latch, String topic, String bootstrapServer, String groupId){
        this.latch = latch;

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        this.consumer = new KafkaConsumer<String, String>(props);
        this.consumer.subscribe(Collections.singleton(topic));
    }

    public void run(){
     try {
         while (true) {
             ConsumerRecords<String, String> records = consumer.poll(100);
             for (ConsumerRecord record : records) {
                 logger.info("Key : " + record.key() + " Topic: " + record.topic() + " message: " + record.value() + "\n");
                 logger.info("Partition : " + record.partition() + " Offset: " + record.offset() + "\n\n");
             }
         }
     }catch (WakeupException ex){
         logger.info("Consumer Poll interrupted "+ex.getLocalizedMessage());
     }finally {
         consumer.close();
         // tell main code to let shut gracefully.
         latch.countDown();
     }
    }

    // wake up will thow an exception and interrupts the consumer.poll method
    public void shutdown(){
        this.consumer.wakeup();
    }
}
