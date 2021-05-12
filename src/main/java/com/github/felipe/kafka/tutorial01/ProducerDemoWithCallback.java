package com.github.felipe.kafka.tutorial01;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    public static void main(String[] args) {

        Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "0.0.0.0:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", "Hello!");

        producer.send(record, (recordMetadata, e) -> {
            if(e == null){
                log.info("Received new metadata. \n"+
                        "Topic: "+recordMetadata.topic()            + "\n" +
                        "Partition: "+recordMetadata.partition()    + "\n" +
                        "Offset: "+recordMetadata.offset()          + "\n" +
                        "Timestamp: "+recordMetadata.timestamp());
            }else {
                log.error("Error while producing", e);
            }
        });

        producer.flush();

        producer.close();
    }
}
