<!--
Copyright 2024 Deutsche Telekom IT GmbH

SPDX-License-Identifier: Apache-2.0
-->

# Environment variables

Galaxy is configured using environment variables. The following environment variables are supported:

| Name | Default | Description |
|---|---|---|
| GALAXY_KAFKA_BROKERS | kafka:9092 | Kafka broker for publishing and consuming events |
| GALAXY_KAFKA_TRANSACTION_PREFIX | multiplexer | Transaction prefix for publishing events |
| GALAXY_KAFKA_GROUP_ID | multiplexers | Kafka consumer group for publishing events |
| GALAXY_KAFKA_SESSION_TIMEOUT | 90000 | Kafka session timeout in milliseconds |
| GALAXY_KAFKA_GROUP_INSTANCE_ID | multiplexer-0 | Kafka consumer group instance ID |
| GALAXY_KAFKA_CONSUMING_TOPIC | published | Kafka topic for consuming events |
| GALAXY_KAFKA_STATUS_TOPIC | status | Kafka topic for status messages |
| GALAXY_KAFKA_PARTITION_COUNT | 10 | Number of partitions for Kafka |
| GALAXY_KAFKA_TRANSACTION_TIMEOUT | 1000 | Transaction timeout for Kafka in milliseconds |
| GALAXY_KAFKA_AUTO_CREATE_TOPICS | false | Auto-create Kafka topics |
| GALAXY_KAFKA_AUTO_OFFSET_RESET | earliest | Auto-offset reset for Kafka |
| GALAXY_KAFKA_AUTO_COMMIT | false | Auto-commit for Kafka |
| GALAXY_KAFKA_LINGER_MS | 0 | Linger time for Kafka |
| GALAXY_KAFKA_ACKS | 1 | Number of acknowledgments for Kafka |
| GALAXY_KAFKA_COMPRESSION_ENABLED | false | Enable compression for Kafka |
| GALAXY_KAFKA_COMPRESSION_TYPE | none | Compression type for Kafka |
| GALAXY_CACHE_SERVICE_DNS | app-cache-headless.integration.svc.cluster.local | DNS for cache service |
| GALAXY_CACHE_DE_DUPLICATION_ENABLED | false | Enable deduplication cache |
| GALAXY_CACHE_LOCAL_SUBSCRIPTION_CACHE_ENABLED | true | Enables the pod-local subscription cache |
| GALAXY_CACHE_LOCAL_SUBSCRIPTION_CACHE_FALLBACK_MODE | hazelcast-with-mongo-fallback | Read fallback when the local cache cannot serve reads (`hazelcast-with-mongo-fallback` or `none`). With `none`, stale local entries are served indefinitely if necessary |
| GALAXY_CACHE_LOCAL_SUBSCRIPTION_CACHE_MONGO_HEAD_FALLBACK_ENABLED | true | Uses the MongoDB head when the ZooKeeper head cannot be determined (ZooKeeper mode only) |
| GALAXY_CACHE_LOCAL_SUBSCRIPTION_CACHE_SNAPSHOT_COLLECTION | subscriptions.subscriber.horizon.telekom.de.v1-snapshots | MongoDB collection with the snapshot entries |
| GALAXY_CACHE_LOCAL_SUBSCRIPTION_CACHE_HEAD_COLLECTION | subscriptions.subscriber.horizon.telekom.de.v1-head | MongoDB collection with the head of the active snapshot |
| GALAXY_CACHE_LOCAL_SUBSCRIPTION_CACHE_STALE_LOCAL_CACHE_READ_GRACE_PERIOD | 120s | How long a stale local snapshot may serve reads before Hazelcast is used. Only applies to `FALLBACK_MODE=hazelcast-with-mongo-fallback` |
| GALAXY_CACHE_LOCAL_SUBSCRIPTION_CACHE_REQUIRE_LOCAL_CACHE_AT_STARTUP | true | Whether startup waits for the first local snapshot. Only applies to `FALLBACK_MODE=hazelcast-with-mongo-fallback`; with `none`, startup always waits |
| GALAXY_CACHE_LOCAL_SUBSCRIPTION_CACHE_INITIAL_SNAPSHOT_TIMEOUT | 15s | Maximum wait for the first local snapshot when startup waits for it; afterwards startup fails and the process terminates. `0s` waits indefinitely |
| GALAXY_CACHE_LOCAL_SUBSCRIPTION_CACHE_RECONCILE_INTERVAL | 60s | Interval for re-checking the active head (ZooKeeper or MongoDB); `0s` disables it |
| GALAXY_CACHE_LOCAL_SUBSCRIPTION_CACHE_MONGO_HEAD_POLL_JITTER | 10s | Maximum random offset of the first periodic head reconciliation |
| GALAXY_CACHE_LOCAL_SUBSCRIPTION_CACHE_MONGO_SNAPSHOT_SYNC_JITTER | 10s | Maximum random delay before loading a snapshot for prepared preloads and reconnects (ZooKeeper mode only) |
| GALAXY_CACHE_LOCAL_SUBSCRIPTION_CACHE_ZOO_KEEPER_ENABLED | true | ZooKeeper as head source; `false` polls only the MongoDB head |
| GALAXY_CACHE_LOCAL_SUBSCRIPTION_CACHE_ZOO_KEEPER_ENSEMBLE_TRACKER_ENABLED | true | Lets Curator follow ZooKeeper-published ensemble addresses. Can be `false` for local operation, because the published addresses are not reachable from the host |
| GALAXY_CACHE_LOCAL_SUBSCRIPTION_CACHE_ZOO_KEEPER_CONNECT_STRING | horizon-zookeeper.integration.svc.cluster.local:2181 | ZooKeeper connect string |
| GALAXY_CACHE_LOCAL_SUBSCRIPTION_CACHE_ZOO_KEEPER_PREPARED_PATH | /horizon/subscriptions/prepared | ZNode path of the prepared head |
| GALAXY_CACHE_LOCAL_SUBSCRIPTION_CACHE_ZOO_KEEPER_ACTIVATE_PATH | /horizon/subscriptions/activated | ZNode path of the activated head |
| GALAXY_CACHE_LOCAL_SUBSCRIPTION_CACHE_ZOO_KEEPER_CONNECTION_TIMEOUT | 5s | Curator connection timeout; also bounds each ZooKeeper head read |
| GALAXY_CACHE_LOCAL_SUBSCRIPTION_CACHE_ZOO_KEEPER_SESSION_TIMEOUT | 30s | ZooKeeper session timeout |
| GALAXY_CORE_THREADPOOL_SIZE | 5 | Core size of the thread pool for the galaxy component |
| GALAXY_MAX_THREADPOOL_SIZE | 100 | Maximum size of the thread pool for the galaxy component |
| GALAXY_SUBSCRIPTION_CORE_THREADPOOL_SIZE | 10 | Core size of the thread pool for event subscriptions |
| GALAXY_SUBSCRIPTION_MAX_THREADPOOL_SIZE | 20 | Maximum size of the thread pool for event subscriptions |
| GALAXY_DEFAULT_ENVIRONMENT | default | Default environment for multi-tenancy |