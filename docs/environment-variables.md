<!--
Copyright 2024 Deutsche Telekom IT GmbH

SPDX-License-Identifier: Apache-2.0
-->

## Environment Variables

| Name                                     | Default                                              | Description                                                       |
|------------------------------------------|------------------------------------------------------|-------------------------------------------------------------------|
| GALAXY_KAFKA_BROKERS                     | ``kafka:9092``                                       | Kafka broker for publishing and consuming events                  |
| GALAXY_KAFKA_TRANSACTION_PREFIX          | ``multiplexer``                                      | Transaction prefix for publishing events                          |
| GALAXY_KAFKA_GROUP_ID                    | ``multiplexers``                                     | Kafka consumer group for publishing events                        |
| GALAXY_KAFKA_SESSION_TIMEOUT             | ``90000``                                            | Kafka session timeout in milliseconds                             |
| GALAXY_KAFKA_GROUP_INSTANCE_ID           | ``multiplexer-0``                                    | Kafka consumer group instance ID                                  |
| GALAXY_KAFKA_CONSUMING_TOPIC             | ``published``                                        | Kafka topic for consuming events                                  |
| GALAXY_KAFKA_STATUS_TOPIC                | ``status``                                           | Kafka topic for status messages                                   |
| GALAXY_KAFKA_PARTITION_COUNT             | ``10``                                               | Number of partitions for Kafka                                    |
| GALAXY_KAFKA_TRANSACTION_TIMEOUT         | ``1000``                                             | Transaction timeout for Kafka in milliseconds                     |
| GALAXY_KAFKA_AUTO_CREATE_TOPICS          | ``false``                                            | Auto-create Kafka topics                                          |
| GALAXY_KAFKA_AUTO_OFFSET_RESET           | ``earliest``                                         | Auto-offset reset for Kafka                                       |
| GALAXY_KAFKA_AUTO_COMMIT                 | ``false``                                            | Auto-commit for Kafka                                             |
| GALAXY_KAFKA_LINGER_MS                   | ``0``                                                | Linger time for Kafka                                             |
| GALAXY_KAFKA_ACKS                        | ``1``                                                | Number of acknowledgments for Kafka                               |
| GALAXY_KAFKA_COMPRESSION_ENABLED         | ``false``                                            | Enable compression for Kafka                                      |                     
| GALAXY_KAFKA_COMPRESSION_TYPE            | ``none``                                             | Compression type for Kafka                                        |
| GALAXY_CACHE_SERVICE_DNS                 | ``app-cache-headless.integration.svc.cluster.local`` | DNS for cache service                                             |
| GALAXY_CACHE_DE_DUPLICATION_ENABLED      | ``false``                                            | Enable deduplication cache                                        |
| GALAXY_CORE_THREADPOOL_SIZE              | ``5``                                                | Core size of the thread pool for the galaxy component             |
| GALAXY_MAX_THREADPOOL_SIZE               | ``100``                                              | Maximum size of the thread pool for the galaxy component          |
| GALAXY_SUBSCRIPTION_CORE_THREADPOOL_SIZE | ``10``                                               | Core size of the thread pool for event subscriptions              |
| GALAXY_SUBSCRIPTION_MAX_THREADPOOL_SIZE  | ``20``                                               | Maximum size of the thread pool for event subscriptions           |
| GALAXY_DEFAULT_ENVIRONMENT               | ``default``                                          | Default environment for multi-tenancy                             |