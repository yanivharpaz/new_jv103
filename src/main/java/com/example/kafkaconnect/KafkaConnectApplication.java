package com.example.kafkaconnect;

import com.example.kafkaconnect.config.AppConfig;
import com.example.kafkaconnect.connector.ElasticsearchSinkConnector;
import com.example.kafkaconnect.service.KafkaConsumerService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestHighLevelClient;

@Slf4j
public class KafkaConnectApplication {
    public static void main(String[] args) {
        try {
            AppConfig config = new AppConfig();
            ObjectMapper objectMapper = new ObjectMapper();
            RestHighLevelClient elasticsearchClient = config.createElasticsearchClient();
            
            ElasticsearchSinkConnector connector = new ElasticsearchSinkConnector(
                elasticsearchClient, 
                objectMapper,
                config.getElasticsearchBulkActions()
            );
            
            KafkaConsumerService consumerService = new KafkaConsumerService(
                config.createKafkaConsumer(),
                connector,
                config.getKafkaTopic(),
                config.getBatchSize(),
                config.getBatchTimeoutMs()
            );
            
            // Add shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutting down application...");
                consumerService.shutdown();
                try {
                    elasticsearchClient.close();
                } catch (Exception e) {
                    log.error("Error closing Elasticsearch client", e);
                }
            }));
            
            consumerService.start();
            
        } catch (Exception e) {
            log.error("Error starting application", e);
            System.exit(1);
        }
    }
} 