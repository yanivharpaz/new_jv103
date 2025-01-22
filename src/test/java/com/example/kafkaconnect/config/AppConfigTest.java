package com.example.kafkaconnect.config;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Path;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class AppConfigTest {

    @Test
    void shouldLoadPropertiesSuccessfully() {
        // When
        AppConfig config = new AppConfig();

        // Then
        assertNotNull(config.getKafkaTopic());
        assertTrue(config.getBatchSize() > 0);
        assertTrue(config.getBatchTimeoutMs() > 0);
        assertTrue(config.getElasticsearchBulkActions() > 0);
    }

    @Test
    void shouldCreateKafkaConsumer() {
        // When
        AppConfig config = new AppConfig();
        KafkaConsumer<String, String> consumer = config.createKafkaConsumer();

        // Then
        assertNotNull(consumer);
    }

    @Test
    void shouldCreateElasticsearchClient() {
        // When
        AppConfig config = new AppConfig();
        RestHighLevelClient client = config.createElasticsearchClient();

        // Then
        assertNotNull(client);
    }

    @Test
    void shouldThrowExceptionForMissingProperty() {
        // Given
        Properties props = new Properties();
        // Missing required property

        // When/Then
        assertThrows(RuntimeException.class, () -> {
            AppConfig config = new AppConfig();
            config.getBatchSize();
        });
    }

    @Test
    void shouldThrowExceptionForInvalidIntegerProperty(@TempDir Path tempDir) throws Exception {
        // Given
        File propsFile = tempDir.resolve("test.properties").toFile();
        try (FileWriter writer = new FileWriter(propsFile)) {
            writer.write("kafka.batch.size=invalid");
        }

        // When/Then
        assertThrows(RuntimeException.class, () -> {
            AppConfig config = new AppConfig();
            config.getBatchSize();
        });
    }
} 