package de.telekom.horizon.galaxy.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.telekom.eni.pandora.horizon.cache.service.DeDuplicationService;
import de.telekom.eni.pandora.horizon.kafka.config.KafkaProperties;
import de.telekom.eni.pandora.horizon.kafka.event.EventWriter;
import de.telekom.eni.pandora.horizon.metrics.HorizonMetricsHelper;
import de.telekom.eni.pandora.horizon.model.event.PublishedEventMessage;
import de.telekom.eni.pandora.horizon.tracing.HorizonTracer;
import de.telekom.eni.pandora.horizon.victorialog.client.VictoriaLogClient;
import de.telekom.horizon.galaxy.cache.PayloadSizeHistogramCache;
import de.telekom.horizon.galaxy.cache.SubscriptionCache;
import de.telekom.horizon.galaxy.config.GalaxyConfig;
import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * Factory for creating tasks associated with a published message.
 * @see PublishedEventMessage
 * @see PublishedMessageTask
 */
@Component
@Getter
public class PublishedMessageTaskFactory {

    private final VictoriaLogClient victoriaLogClient;
    private final HorizonTracer tracer;
    private final EventWriter eventWriter;
    private final HorizonMetricsHelper metricsHelper;
    private final SubscriptionCache subscriptionCache;
    private final DeDuplicationService deDuplicationService;
    private final KafkaProperties kafkaProperties;
    private final PayloadSizeHistogramCache incomingPayloadSizeCache;
    private final PayloadSizeHistogramCache outgoingPayloadSizeHistogramCache;
    private final GalaxyConfig galaxyConfig;
    private final ObjectMapper objectMapper;

    @Autowired
    public PublishedMessageTaskFactory(VictoriaLogClient victoriaLogClient, HorizonTracer tracer, EventWriter eventWriter, HorizonMetricsHelper metricsHelper, SubscriptionCache subscriptionCache, DeDuplicationService deDuplicationService, KafkaProperties kafkaProperties, @Qualifier("incomingPayloadSizeCache") PayloadSizeHistogramCache incomingPayloadSizeCache, @Qualifier("outgoingPayloadSizeCache") PayloadSizeHistogramCache outgoingPayloadSizeHistogramCache, GalaxyConfig galaxyConfig, ObjectMapper objectMapper) {
        this.victoriaLogClient = victoriaLogClient;
        this.tracer = tracer;
        this.eventWriter = eventWriter;
        this.metricsHelper = metricsHelper;
        this.subscriptionCache = subscriptionCache;
        this.deDuplicationService = deDuplicationService;
        this.kafkaProperties = kafkaProperties;
        this.incomingPayloadSizeCache = incomingPayloadSizeCache;
        this.outgoingPayloadSizeHistogramCache = outgoingPayloadSizeHistogramCache;
        this.galaxyConfig = galaxyConfig;
        this.objectMapper = objectMapper;
    }

    public PublishedMessageTask newTask(ConsumerRecord<String, String> consumerRecord) {
        return new PublishedMessageTask(consumerRecord, this);
    }
}
