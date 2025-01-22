// package com.example.kafkaconnect.config;

// import org.apache.http.HttpHost;
// import org.elasticsearch.client.RestClient;
// import org.elasticsearch.client.RestHighLevelClient;
// import lombok.extern.slf4j.Slf4j;

// @Slf4j
// public class ConnectorConfig {
//     private final AppConfig appConfig;
    
//     public ConnectorConfig(AppConfig appConfig) {
//         this.appConfig = appConfig;
//     }
    
//     public RestHighLevelClient elasticsearchClient() {
//         String[] hosts = appConfig.getProperty("elasticsearch.hosts").split(",");
//         HttpHost[] httpHosts = new HttpHost[hosts.length];
        
//         for (int i = 0; i < hosts.length; i++) {
//             String[] hostParts = hosts[i].split(":");
//             httpHosts[i] = new HttpHost(hostParts[0], 
//                 Integer.parseInt(hostParts[1]), "http");
//         }
        
//         return new RestHighLevelClient(
//             RestClient.builder(httpHosts)
//                 .setRequestConfigCallback(requestConfigBuilder ->
//                     requestConfigBuilder
//                         .setConnectTimeout(appConfig.getIntProperty("elasticsearch.connection-timeout"))
//                         .setSocketTimeout(appConfig.getIntProperty("elasticsearch.socket-timeout"))
//                 )
//         );
//     }
// } 