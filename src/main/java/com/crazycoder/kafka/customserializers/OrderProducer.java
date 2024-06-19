package com.crazycoder.kafka.customserializers;

import com.crazycoder.kafka.OrderCallback;
import com.crazycoder.kafka.customserializers.partitioners.VIPPartitioner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class OrderProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
//        props.setProperty("bootstrap.servers", "localhost:9092");
//        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
//        props.setProperty("value.serializer", OrderSerializer.class.getName());
//        props.setProperty("partitioner.class", VIPPartitioner.class.getName());

        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, OrderSerializer.class.getName());
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG,"34443444");
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        props.setProperty(ProducerConfig.RETRIES_CONFIG, "1");
        props.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "400");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "1098308290");
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "200");
        props.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "200");
        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");


        props.setProperty("partitioner.class", VIPPartitioner.class.getName());

        KafkaProducer<String, Order> producer = new KafkaProducer<>(props);
        Order order = new Order();
        order.setCustomerName("John");
        order.setProduct("Iphone 13 PRo");
        order.setQuantity(1);
        ProducerRecord<String, Order> record = new ProducerRecord<>("OrderPartitionedTopic", order.getCustomerName(), order);

        //Method 1 : Fire and Forget
        try {
            producer.send(record);
            System.out.println("Message has been successfully send");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }

    }
}
