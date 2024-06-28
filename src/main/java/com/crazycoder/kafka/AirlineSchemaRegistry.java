package com.crazycoder.kafka;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class AirlineSchemaRegistry {
    private static final String SCHEMA_REGISTRY_URL = "http://schema-registry-host:port";
    private static final String TOPIC_NAME = "example-topic";

    public static void main(String[] args) {
        //JSON Message
        /*{
            "orderId": "12345",
                "productId": "67890",
                "quantity": 2,
                "price": 19.99
        }*/

        //Avro
      /*  {
            "type": "record",
                "name": "Order",
                "fields": [
            {"name": "orderId", "type": "string"},
            {"name": "productId", "type": "string"},
            {"name": "quantity", "type": "int"},
            {"name": "price", "type": "float"}
  ]
        }
*/


        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.put("group.id", "test-consumer-group");
        props.put("schema.registry.url", SCHEMA_REGISTRY_URL);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(TOPIC_NAME));

        try {
            while (true) {
                for (ConsumerRecord<String, GenericRecord> record : consumer.poll(Duration.ofMillis(100))) {
                    // Retrieve schema ID from the record
                    int schemaId = (int) record.headers().lastHeader("schemaId").value()[0];

                    // Retrieve schema from Schema Registry
                    Schema schema = new SchemaRegistryClient(SCHEMA_REGISTRY_URL).getById(schemaId);

                    // Deserialize record using retrieved schema
                    GenericRecord data = record.value();
                    // Assuming you know the schema structure, you can access fields like this:
                    String field1 = (String) data.get("field1");

                    System.out.printf("Received message: field1=%s%n", field1);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
