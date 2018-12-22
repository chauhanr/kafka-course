package com.github.kafka.course;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ApplicationWithCallback {
    private static Logger logger = LoggerFactory.getLogger(ApplicationWithCallback.class);

    public static void main(String[] args) {

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create a producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        // create producer record
        for (int i=0; i<10; i++) {
            String msg = "Producer Message "+i;
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>("com.mytopic.metric", msg);
            // send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if ( e == null){
                        logger.info("Received Metadata. \nTopic"+recordMetadata.topic()+"\n"+
                                "Partiton :"+recordMetadata.partition()+"\n"+
                                "Offset : "+recordMetadata.offset()+"\n");
                    }else{
                        logger.error("Error producing message "+e.getMessage());
                    }
                }
            });
            producer.flush();
        }
        producer.close();
    }
}
