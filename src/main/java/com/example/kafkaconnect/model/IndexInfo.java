package com.example.kafkaconnect.model;

import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class IndexInfo {
    private String indexName;
    private String aliasName;
    private String productType;
    private int counter;
} 