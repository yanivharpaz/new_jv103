package com.example.kafkaconnect.service;

import com.example.kafkaconnect.connector.ElasticsearchSinkConnector;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Slf4j
public class KafkaConsumerService {
    private final KafkaConsumer<String, String> consumer;
    private final ElasticsearchSinkConnector connector;
    private final String topic;
    private final int batchSize;
    private final Duration pollTimeout;
    private final AtomicBoolean running;
    
    public KafkaConsumerService(
        KafkaConsumer<String, String> consumer,
        ElasticsearchSinkConnector connector,
        String topic,
        int batchSize,
        int timeoutMs
    ) {
        this.consumer = consumer;
        this.connector = connector;
        this.topic = topic;
        this.batchSize = batchSize;
        this.pollTimeout = Duration.ofMillis(timeoutMs);
        this.running = new AtomicBoolean(true);
    }
    
    public void start() {
        try {
            consumer.subscribe(Collections.singletonList(topic));
            
            while (running.get()) {
                ConsumerRecords<String, String> records = 
                    consumer.poll(pollTimeout);
                
                if (!records.isEmpty()) {
                    List<String> messages = StreamSupport
                        .stream(records.spliterator(), false)
                        .map(record -> record.value())
                        .collect(Collectors.toList());
                    
                    connector.processMessages(messages);
                    consumer.commitSync();
                }
            }
        } catch (Exception e) {
            log.error("Error in consumer service", e);
            throw new RuntimeException("Consumer service failed", e);
        } finally {
            consumer.close();
        }
    }
    
    public void shutdown() {
        running.set(false);
    }
} 