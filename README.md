# Kafka Connect Elasticsearch Sink

A robust Java application that connects Apache Kafka to Elasticsearch, featuring intelligent index management and aliasing capabilities.

## Features

- Consumes messages from Kafka topics
- Automatically creates and manages Elasticsearch indices and aliases
- Smart index naming convention with product type support
- Bulk processing capabilities for optimal performance
- Automatic alias management and caching
- Fault-tolerant message processing
- Configurable batch sizes and timeouts

## Prerequisites

- Java 17 or higher
- Docker and Docker Compose (for local development)
- Gradle 8.x

## Technology Stack

- Java 17
- Apache Kafka 3.6.1
- Elasticsearch 6.8.23
- Spring Framework (core dependencies)
- Project Lombok
- JUnit 5 & Mockito for testing
- Testcontainers for integration testing

## Getting Started

### 1. Clone the Repository 