package com.example.kafkaconnect.connector;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.elasticsearch.client.IndicesClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;
import java.util.ArrayList;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
class ElasticsearchSinkConnectorTest {

    private static final int BULK_ACTIONS = 100;

    @Mock
    private RestHighLevelClient elasticsearchClient;

    @Mock
    private IndicesClient indicesClient;

    @Mock
    private BulkResponse bulkResponse;

    @Captor
    private ArgumentCaptor<BulkRequest> bulkRequestCaptor;

    private ElasticsearchSinkConnector connector;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        connector = new ElasticsearchSinkConnector(
            elasticsearchClient, 
            objectMapper,
            BULK_ACTIONS
        );
        when(elasticsearchClient.indices()).thenReturn(indicesClient);
    }

    @Test
    void shouldProcessBatchSuccessfully() throws Exception {
        // Given
        List<String> messages = Arrays.asList(
            "{\"ProductType\":\"Electronics\",\"id\":\"1\"}",
            "{\"ProductType\":\"Electronics\",\"id\":\"2\"}"
        );
        when(indicesClient.exists(any(GetIndexRequest.class), any(RequestOptions.class)))
            .thenReturn(true);
        when(elasticsearchClient.bulk(any(BulkRequest.class), any(RequestOptions.class)))
            .thenReturn(bulkResponse);
        when(bulkResponse.hasFailures()).thenReturn(false);

        // When
        connector.processMessages(messages);

        // Then
        verify(elasticsearchClient).bulk(bulkRequestCaptor.capture(), any(RequestOptions.class));
        BulkRequest capturedRequest = bulkRequestCaptor.getValue();
        assertEquals(2, capturedRequest.numberOfActions());
    }

    @Test
    void shouldCreateIndexIfNotExists() throws Exception {
        // Given
        List<String> messages = List.of(
            "{\"ProductType\":\"Electronics\",\"id\":\"1\"}"
        );
        when(indicesClient.exists(any(GetIndexRequest.class), any(RequestOptions.class)))
            .thenReturn(false);
        when(elasticsearchClient.bulk(any(BulkRequest.class), any(RequestOptions.class)))
            .thenReturn(bulkResponse);
        when(bulkResponse.hasFailures()).thenReturn(false);

        // When
        connector.processMessages(messages);

        // Then
        verify(indicesClient).create(any(CreateIndexRequest.class), any(RequestOptions.class));
    }

    @Test
    void shouldHandleBulkFailure() throws Exception {
        // Given
        List<String> messages = List.of(
            "{\"ProductType\":\"Electronics\",\"id\":\"1\"}"
        );
        when(indicesClient.exists(any(GetIndexRequest.class), any(RequestOptions.class)))
            .thenReturn(true);
        when(elasticsearchClient.bulk(any(BulkRequest.class), any(RequestOptions.class)))
            .thenReturn(bulkResponse);
        when(bulkResponse.hasFailures()).thenReturn(true);
        when(bulkResponse.buildFailureMessage()).thenReturn("Bulk indexing failed");

        // When/Then
        assertThrows(RuntimeException.class, () -> connector.processMessages(messages));
    }

    @Test
    void shouldHandleInvalidJson() {
        // Given
        List<String> messages = List.of("invalid json");

        // When/Then
        assertThrows(RuntimeException.class, () -> connector.processMessages(messages));
    }

    @Test
    void shouldHandleMissingProductType() {
        // Given
        List<String> messages = List.of(
            "{\"id\":\"1\"}" // Missing ProductType
        );

        // When/Then
        assertThrows(RuntimeException.class, () -> connector.processMessages(messages));
    }

    @Test
    void shouldHandleLargeBatch() throws Exception {
        // Given
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            messages.add(String.format(
                "{\"ProductType\":\"Electronics\",\"id\":\"%d\"}", i));
        }
        when(indicesClient.exists(any(GetIndexRequest.class), any(RequestOptions.class)))
            .thenReturn(true);
        when(elasticsearchClient.bulk(any(BulkRequest.class), any(RequestOptions.class)))
            .thenReturn(bulkResponse);
        when(bulkResponse.hasFailures()).thenReturn(false);

        // When
        connector.processMessages(messages);

        // Then
        verify(elasticsearchClient, atLeast(2))
            .bulk(any(BulkRequest.class), any(RequestOptions.class));
    }

    @Test
    void shouldHandleEmptyBatch() throws IOException {
        // Given
        List<String> messages = Collections.emptyList();

        // When
        connector.processMessages(messages);

        // Then
        verify(elasticsearchClient, never())
            .bulk(any(BulkRequest.class), any(RequestOptions.class));
    }

    @Test
    void shouldHandleNullProductType() {
        // Given
        List<String> messages = List.of(
            "{\"id\":\"1\",\"ProductType\":null}"
        );

        // When/Then
        assertThrows(RuntimeException.class, () -> connector.processMessages(messages));
    }
} 