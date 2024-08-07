// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.telekom.eni.pandora.horizon.cache.service.JsonCacheService;
import de.telekom.eni.pandora.horizon.kafka.event.EventWriter;
import de.telekom.eni.pandora.horizon.kubernetes.resource.SubscriptionResource;
import de.telekom.eni.pandora.horizon.model.event.Event;
import de.telekom.eni.pandora.horizon.model.event.PublishedEventMessage;
import de.telekom.eni.pandora.horizon.model.event.SubscriptionEventMessage;
import de.telekom.eni.pandora.horizon.model.meta.EventRetentionTime;
import de.telekom.horizon.galaxy.cache.SubscriberCache;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@SpringBootTest
public abstract class AbstractIntegrationTest {

    @MockBean
    JsonCacheService<SubscriptionResource> jsonCacheService;

    @SpyBean
    public SubscriberCache subscriptionCacheMock;

    protected final static String TEST_ENVIRONMENT = "playground";
    private final static String TRACING_HEADER_NAME = "x-b3-traceid";

    static {
        EmbeddedKafkaHolder.getEmbeddedKafka();
    }

    public static final EmbeddedKafkaBroker broker = EmbeddedKafkaHolder.getEmbeddedKafka();

    private final Map<String, BlockingQueue<ConsumerRecord<String, String>>> multiplexedRecordsMap = new HashMap<>();

    private KafkaMessageListenerContainer<String, String> container;

    private String eventType;

    @Autowired
    private EventWriter eventWriter;

    @Autowired
    private ConsumerFactory consumerFactory;

    @Value("${horizon.kafka.consumingTopic}")
    private String consumingTopic;

    @Autowired
    public ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        eventType = "junit.test.event." + DigestUtils.sha1Hex(String.valueOf(System.currentTimeMillis()));

        multiplexedRecordsMap.putIfAbsent(getEventType(), new LinkedBlockingQueue<>());

        var topicNames = Arrays.stream(EventRetentionTime.values()).map(EventRetentionTime::getTopic).distinct().toArray(String[]::new);
        ContainerProperties containerProperties = new ContainerProperties(topicNames);
        containerProperties.setGroupId(eventType);
        containerProperties.setAckMode(ContainerProperties.AckMode.RECORD);
        container = new KafkaMessageListenerContainer<>(consumerFactory, containerProperties);
        container.setupMessageListener((MessageListener<String, String>) record ->
            multiplexedRecordsMap.get(getEventType()).add(record)
        );
        container.start();

        var partitionsForAllTopics = topicNames.length * broker.getPartitionsPerTopic();
        ContainerTestUtils.waitForAssignment(container, partitionsForAllTopics);
    }

    @AfterEach
    void tearDown() {
        container.stop();
    }

    public void simulateNewPublishedEvent(PublishedEventMessage message) throws JsonProcessingException {
        eventWriter.send(consumingTopic, message);
    }

    @DynamicPropertySource
    static void dynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.zipkin.enabled", () -> false);
        registry.add("spring.zipkin.baseUrl", () -> "http://localhost:9411");
        registry.add("horizon.kafka.partitionCount", () -> 1);
        registry.add("horizon.kafka.autoCreateTopics", () -> true);
        registry.add("horizon.cache.kubernetesServiceDns", () -> "");
        registry.add("horizon.cache.deDuplication.enabled", () -> true);
        registry.add("kubernetes.enabled", () -> false);
    }

    public ConsumerRecord<String, String> pollForRecord(int timeout, TimeUnit timeUnit) throws InterruptedException {
        return multiplexedRecordsMap.get(getEventType()).poll(timeout, timeUnit);
    }

    public final String getEventType() {
        return eventType;
    }

    protected List<SubscriptionEventMessage> receiveOutboundEvents() {
        List<SubscriptionEventMessage> receivedMessages = new LinkedList<>();

        ConsumerRecord<String, String> received;
        try {
            while ((received = pollForRecord(5, TimeUnit.SECONDS)) != null) {
                receivedMessages.add(objectMapper.readValue(received.value(), SubscriptionEventMessage.class));
            }
        } catch (JsonProcessingException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        return receivedMessages;
    }

    @NotNull
    protected PublishedEventMessage getPublishedEventMessage(Object testEvent) {
        String eventId = UUID.randomUUID().toString();
        String traceId = UUID.randomUUID().toString();
        String messageUuid = UUID.randomUUID().toString();


        PublishedEventMessage message = new PublishedEventMessage();
        message.setUuid(messageUuid);
        message.setEnvironment(TEST_ENVIRONMENT);
        message.setHttpHeaders(
                Map.of("X-Pandora-Type", List.of("Test"), TRACING_HEADER_NAME, List.of(traceId))
        );

        Event event = new Event();
        event.setType(getEventType());
        event.setId(eventId);
        event.setData(testEvent);
        message.setEvent(event);
        message.setAdditionalFields(Map.of(TRACING_HEADER_NAME, traceId));
        return message;
    }
}
