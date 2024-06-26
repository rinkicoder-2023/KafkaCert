package com.crazycoder.kafka.customserializers;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class AirlineBookingProducer {

    private static final String TOPIC_NAME = "airline-bookings";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("max.in.flight.requests.per.connection", "1"); // Set to 1 for single in-flight request for simplicity
        props.put("acks", "1");
        //props.put(ProducerConfig.ACKS_CONFIG, "all"); // broker settng min.insync.replicas=2, the write is successful as there are still 2 in-sync replicas acknowledging the write.
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000); //30 sec
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120000); //2 min
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip"); // others are lz4, snappy, zstd
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); //16kb - accumulated size of records for a partition reaches 16 KB, the batch will be sent immediately.
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0); // 10ms - producer will wait up to 10 ms for more records to be added to the batch before sending it, even if the batch is not full.
        props.put(ProducerConfig.RETRIES_CONFIG, 3); // retry attempts
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100); // 100ms backoff between retries
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 67108864);  // 64 MB buffer memory
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000);  // 60seconds wait for space to become available
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "my-first-producer");  // 60seconds wait for space to become available

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            // Simulate booking requests
            for (int i = 0; i < 10; i++) {
                final String bookingRequest = generateBookingRequest(i);
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "booking-" + i, bookingRequest);
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            exception.printStackTrace();
                        } else {
                            System.out.println("Booking request sent: " + bookingRequest);
                            System.out.println("Partition: " + metadata.partition() + ", Offset: " + metadata.offset());
                        }
                    }
                });
                Thread.sleep(1000); // Simulate some delay between booking requests
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String generateBookingRequest(int bookingId) {
        // Simulate a booking request
        return "Booking ID: " + bookingId + ", Flight: Swiss, Passenger: Varun";
    }
}
