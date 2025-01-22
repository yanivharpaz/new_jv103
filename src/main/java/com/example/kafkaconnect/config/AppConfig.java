package com.example.kafkaconnect.config;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class AppConfig {
    private final Properties properties;
    
    public AppConfig() {
        this.properties = loadProperties();
    }
    
    protected Properties loadProperties() {
        Properties props = new Properties();
        try (InputStream input = getClass().getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new RuntimeException("Unable to find application.properties");
            }
            props.load(input);
        } catch (IOException e) {
            log.error("Error loading properties", e);
            throw new RuntimeException("Failed to load configuration", e);
        }
        return props;
    }
    
    public KafkaConsumer<String, String> createKafkaConsumer() {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
            properties.getProperty("kafka.bootstrap.servers"));
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, 
            properties.getProperty("kafka.group.id"));
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, 
            properties.getProperty("kafka.auto.offset.reset"));
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
            StringDeserializer.class.getName());
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
            StringDeserializer.class.getName());
        
        return new KafkaConsumer<>(kafkaProps);
    }
    
    public RestHighLevelClient createElasticsearchClient() {
        String[] hosts = properties.getProperty("elasticsearch.hosts").split(",");
        HttpHost[] httpHosts = new HttpHost[hosts.length];
        
        for (int i = 0; i < hosts.length; i++) {
            String[] hostParts = hosts[i].split(":");
            httpHosts[i] = new HttpHost(hostParts[0], 
                Integer.parseInt(hostParts[1]), "http");
        }
        
        return new RestHighLevelClient(
            RestClient.builder(httpHosts)
                .setRequestConfigCallback(requestConfigBuilder ->
                    requestConfigBuilder
                        .setConnectTimeout(getIntProperty("elasticsearch.connection-timeout"))
                        .setSocketTimeout(getIntProperty("elasticsearch.socket-timeout"))
                )
        );
    }
    
    public String getKafkaTopic() {
        return properties.getProperty("kafka.topic");
    }
    
    public int getBatchSize() {
        return getIntProperty("kafka.batch.size");
    }
    
    public int getBatchTimeoutMs() {
        return getIntProperty("kafka.batch.timeout-ms");
    }
    
    public int getElasticsearchBulkActions() {
        return getIntProperty("elasticsearch.bulk.actions");
    }
    
    private int getIntProperty(String key) {
        String value = properties.getProperty(key);
        if (value == null) {
            throw new RuntimeException("Missing required property: " + key);
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Invalid integer value for property: " + key);
        }
    }
} 