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

public class ConsumerApplicationWithGroup {
    private static Logger logger = LoggerFactory.getLogger(ConsumerApplicationWithGroup.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "myapp-first");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // subscribing
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singleton("com.mytopic.metric"));

        while (true){
            ConsumerRecords<String,String> records = consumer.poll(100) ;
            for (ConsumerRecord record : records){
                logger.info("Key : "+record.key()+ " Topic: "+record.topic()+" message: "+record.value()+"\n");
                logger.info("Partition : "+record.partition()+ " Offset: "+record.offset()+"\n\n");
            }
        }
    }

}
