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
        return "Booking ID: " + bookingId + ", Flight: AirCanada2417, Passenger: Dede jayo";
    }
}
