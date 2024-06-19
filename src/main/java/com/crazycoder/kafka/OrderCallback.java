package com.crazycoder.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;

public class OrderCallback implements org.apache.kafka.clients.producer.Callback {
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
        System.out.println("RecordMeta Data Partition: "+recordMetadata.partition());
        System.out.println("RecordMeta Data Offset: "+recordMetadata.offset());
        System.out.println("Message has been successfully send");

        if(exception != null) {
            exception.printStackTrace();
        }
    }
}
