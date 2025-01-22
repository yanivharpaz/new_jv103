package com.example.kafkaconnect.connector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import com.example.kafkaconnect.service.ElasticsearchService;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ElasticsearchSinkConnector {
    private final RestHighLevelClient elasticsearchClient;
    private final ObjectMapper objectMapper;
    private final int bulkActions;
    private final Map<String, Boolean> indexCache;
    private final ElasticsearchService elasticsearchService;
    
    public ElasticsearchSinkConnector(
        RestHighLevelClient elasticsearchClient,
        ObjectMapper objectMapper,
        int bulkActions
    ) {
        this.elasticsearchClient = elasticsearchClient;
        this.objectMapper = objectMapper;
        this.bulkActions = bulkActions;
        this.indexCache = new ConcurrentHashMap<>();
        this.elasticsearchService = new ElasticsearchService(elasticsearchClient);
    }
    
    public void processMessages(List<String> messages) {
        if (messages == null || messages.isEmpty()) {
            log.debug("No messages to process");
            return;
        }

        try {
            BulkRequest bulkRequest = new BulkRequest();
            
            for (String message : messages) {
                try {
                    elasticsearchService.indexDocument(message);
                } catch (Exception e) {
                    log.error("Error processing message: {}", message, e);
                }
            }
            
        } catch (Exception e) {
            log.error("Error processing batch of {} messages", messages.size(), e);
            throw new RuntimeException("Failed to process message batch", e);
        }
    }
    
    private void executeBulkRequest(BulkRequest bulkRequest) throws Exception {
        BulkResponse bulkResponse = elasticsearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        if (bulkResponse.hasFailures()) {
            log.error("Bulk indexing has failures: {}", bulkResponse.buildFailureMessage());
            throw new RuntimeException("Bulk indexing failed");
        }
        log.info("Successfully indexed {} documents", bulkRequest.numberOfActions());
    }
    
    private void ensureIndexExists(String indexName) throws Exception {
        if (!indexCache.containsKey(indexName)) {
            boolean aliasExists = elasticsearchClient.indices()
                .existsAlias(new GetAliasesRequest().aliases(indexName), RequestOptions.DEFAULT);
                
            if (!aliasExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
                elasticsearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("Created new index: {}", indexName);
            }
            indexCache.put(indexName, true);
        }
    }
} 