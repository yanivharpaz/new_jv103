package com.example.kafkaconnect.integration;

import com.example.kafkaconnect.config.AppConfig;
import com.example.kafkaconnect.connector.ElasticsearchSinkConnector;
import com.example.kafkaconnect.service.KafkaConsumerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@Testcontainers
class ElasticsearchSinkConnectorIntegrationTest {

    @Container
    static final KafkaContainer kafka = new KafkaContainer(
        DockerImageName.parse("confluentinc/cp-kafka:latest")
    );

    @Container
    static final ElasticsearchContainer elasticsearch = new ElasticsearchContainer(
        "docker.elastic.co/elasticsearch/elasticsearch:6.8.23"
    );

    private KafkaProducer<String, String> producer;
    private KafkaConsumerService consumerService;
    private RestHighLevelClient elasticsearchClient;
    private ExecutorService executorService;

    @BeforeEach
    void setUp() {
        // Create test properties
        Properties testProperties = new Properties();
        testProperties.setProperty("kafka.bootstrap.servers", kafka.getBootstrapServers());
        testProperties.setProperty("kafka.topic", "my-topic");
        testProperties.setProperty("elasticsearch.hosts", 
            elasticsearch.getHost() + ":" + elasticsearch.getMappedPort(9200));
        testProperties.setProperty("kafka.batch.size", "100");
        testProperties.setProperty("kafka.batch.timeout-ms", "5000");
        testProperties.setProperty("elasticsearch.bulk.actions", "100");
        testProperties.setProperty("elasticsearch.connection-timeout", "5000");
        testProperties.setProperty("elasticsearch.socket-timeout", "3000");
        
        // Create producer
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(producerProps);

        // Create consumer service
        AppConfig appConfig = new TestAppConfig(testProperties);
        elasticsearchClient = appConfig.createElasticsearchClient();
        ElasticsearchSinkConnector connector = new ElasticsearchSinkConnector(
            elasticsearchClient,
            new ObjectMapper(),
            appConfig.getElasticsearchBulkActions()
        );
        
        consumerService = new KafkaConsumerService(
            appConfig.createKafkaConsumer(),
            connector,
            appConfig.getKafkaTopic(),
            appConfig.getBatchSize(),
            appConfig.getBatchTimeoutMs()
        );

        // Start consumer service in background
        executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> consumerService.start());
    }

    @AfterEach
    void tearDown() throws Exception {
        producer.close();
        consumerService.shutdown();
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.SECONDS);
        elasticsearchClient.close();
    }

    @Test
    void shouldProcessMessageAndIndexToElasticsearch() throws Exception {
        // Given
        String message = "{\"ProductType\":\"Electronics\",\"id\":\"test-1\"}";

        // When
        producer.send(new ProducerRecord<>(
            "test-topic", 
            message
        )).get();

        // Then
        await()
            .atMost(10, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                // Add assertions to verify document was indexed
                // You can use elasticsearchClient.search() here
            });
    }

    // Test AppConfig that uses provided properties
    private static class TestAppConfig extends AppConfig {
        private final Properties testProperties;

        TestAppConfig(Properties testProperties) {
            this.testProperties = testProperties;
        }

        @Override
        protected Properties loadProperties() {
            return testProperties;
        }
    }
} 