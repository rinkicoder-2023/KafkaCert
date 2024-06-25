package com.crazycoder.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;

public class AirlineDelayProcessor {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "airline-delay-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> flightEventsStream = builder.stream("flight-events", Consumed.with(Serdes.String(), Serdes.String()));

        KTable<String, String> airlineInfoTable = builder.table("airline-info", Consumed.with(Serdes.String(), Serdes.String()));

        // Map flight events to key by airlineId and extract delay information
        KStream<String, Integer> delaysByAirline = flightEventsStream
                .map((key, value) -> KeyValue.pair(value.split(",")[0], Integer.parseInt(value.split(",")[1])));

        // Compute the total delay by airline
        KTable<String, Integer> totalDelayByAirline = delaysByAirline.groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
                .reduce(Integer::sum, Materialized.as("sum-store"));

        // Compute the count of flights by airline
        KTable<String, Long> countByAirline = delaysByAirline.groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
                .count(Materialized.as("count-store"));

        // Compute the average delay per airline
        KTable<String, Double> avgDelayByAirline = totalDelayByAirline.join(countByAirline,
                (totalDelay, count) -> (double) totalDelay / count,
                Materialized.with(Serdes.String(), Serdes.Double()));

        // Join the average delay with airline info to enrich the data
        KTable<String, String> enrichedAvgDelay = avgDelayByAirline.join(airlineInfoTable,
                (avgDelay, airlineInfo) -> "Airline: " + airlineInfo + ", Avg Delay: " + avgDelay);

        // Write the enriched average delay to an output topic
        enrichedAvgDelay.toStream().to("enriched-avg-delay", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
