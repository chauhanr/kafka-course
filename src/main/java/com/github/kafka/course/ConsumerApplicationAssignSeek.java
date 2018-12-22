package com.github.kafka.course;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerApplicationAssignSeek {
    private static Logger logger = LoggerFactory.getLogger(ConsumerApplicationAssignSeek.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
       // props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "myapp-first");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        String topic ="com.mytopic.metric";

        // subscribing
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        long offsetRead = 15L;
        // assign
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(topicPartition));
        // seek
        consumer.seek(topicPartition,offsetRead);

        int messageToRead = 5;
        boolean doRead = true;

        while (doRead){
            ConsumerRecords<String,String> records = consumer.poll(100) ;
            for (ConsumerRecord record : records){
                logger.info("Key : "+record.key()+ " Topic: "+record.topic()+" message: "+record.value()+"\n");
                logger.info("Partition : "+record.partition()+ " Offset: "+record.offset()+"\n\n");

                messageToRead--;
                if (messageToRead == 0) {
                    doRead = false;
                    break;
                }
            }
        }
    }

}
