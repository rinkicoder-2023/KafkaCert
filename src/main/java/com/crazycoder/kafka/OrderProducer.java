package com.crazycoder.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class OrderProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");

        KafkaProducer<String, Integer> producer = new KafkaProducer<>(props);
        ProducerRecord<String, Integer> record = new ProducerRecord<>("OrderTopic", "Ipad", 106);

        //Method 1 : Fire and Forget
//        try {
//            producer.send(record);
//            System.out.println("Message has been successfully send");
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            producer.close();
//        }

//        //Method 2 : Sync Send, when response is ready , we get metadata, synchronous call, we are waiting for response
//        try {
//            RecordMetadata recordMetadata = producer.send(record).get();
//            System.out.println("RecordMeta Data Partition: "+recordMetadata.partition());
//            System.out.println("RecordMeta Data Offset: "+recordMetadata.offset());
//            System.out.println("Message has been successfully send");
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            producer.close();
//        }

        //Method 3 : Async, it doesnt wait, whenever order callback is there
        try {
            producer.send(record, new OrderCallback());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
