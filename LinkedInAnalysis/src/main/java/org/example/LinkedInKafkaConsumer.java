package org.example;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class LinkedInKafkaConsumer {
    private static final String TOPIC = "nagachakradharg_linkedin_livefeed";
    private static final String KAFKABROKERS = "wn0-kafka.m0ucnnwuiqae3jdorci214t2mf.bx.internal.cloudapp.net:9092,wn1-kafka.m0ucnnwuiqae3jdorci214t2mf.bx.internal.cloudapp.net:9092";

    public static void main(String[] args) {
        // Set up consumer properties
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKABROKERS);
        properties.put("group.id", "linkedin-job-consumer-group");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());
        properties.put("auto.offset.reset", "earliest"); // Start reading from beginning of topic

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to topic
        consumer.subscribe(Collections.singletonList(TOPIC));

        try {
            while (true) {
                // Poll for new data
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Offset = %d, Key = %s, Value = %s%n",
                            record.offset(), record.key(), record.value());
                }
            }
        } catch (Exception e) {
            System.err.println("Error while consuming: " + e.getMessage());
        } finally {
            consumer.close();
        }
    }
}