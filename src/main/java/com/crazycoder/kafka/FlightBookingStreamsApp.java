package com.crazycoder.kafka;

import java.util.Properties;

public class FlightBookingStreamsApp {

    public static void main(String[] args) {
        // Set the properties for the Kafka Streams application
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "flight-booking-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> sourceStream = builder.stream("flight-bookings");

        KStream<String, String>[] branches = sourceStream.branch(
                (key, value) -> isInternational(value),  // International booking
                (key, value) -> true                     // Domestic booking
        );

        branches[0].to("international-bookings", Produced.with(Serdes.String(), Serdes.String()));
        branches[1].to("domestic-bookings", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    // Simple check to determine if the booking is international
    private static boolean isInternational(String booking) {
        // Assume the booking string contains a country code for simplicity
        // In a real application, parse the booking JSON or object to extract the relevant field
        return booking.contains("INTERNATIONAL");
    }
}
