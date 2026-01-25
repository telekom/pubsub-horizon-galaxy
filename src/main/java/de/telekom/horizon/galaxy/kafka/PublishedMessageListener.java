// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.kafka;

import de.telekom.eni.pandora.horizon.model.event.PublishedEventMessage;
import de.telekom.eni.pandora.horizon.tracing.HorizonTracer;
import de.telekom.horizon.galaxy.config.GalaxyConfig;
import de.telekom.horizon.galaxy.model.PublishedMessageTaskResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * The {@code PublishedMessageListener} class is responsible for processing Kafka messages in batches.
 * <p>
 * The class handles new {@link ConsumerRecord} objects containing {@link PublishedEventMessage} objects.
 * For each {@link ConsumerRecord} a task is created using the {@link PublishedMessageTaskFactory}.
 * If any task in the list fails, a nack (negative acknowledgment) for the failed message
 * and all following messages in the batch is sent to Kafka. All messages before the failed message are getting acknowledged.
 * If all tasks are successful, an acknowledgment for the batch is sent to Kafka.
 */
@Slf4j
public class PublishedMessageListener extends AbstractConsumerSeekAware implements BatchAcknowledgingMessageListener<String, String> {

    private final PublishedMessageTaskFactory publishedMessageTaskFactory;
    private final HorizonTracer tracer;


    public PublishedMessageListener(PublishedMessageTaskFactory publishedMessageTaskFactory, HorizonTracer horizonTracer, GalaxyConfig galaxyConfig) {
        super();
        this.publishedMessageTaskFactory = publishedMessageTaskFactory;
        this.tracer = horizonTracer;

    }

    /**
     * Handles a batch of messages received from Kafka.
     *
     * @param consumerRecords the records received from Kafka
     * @param acknowledgment  the acknowledgment object used to nack or ack the batch (partially)
     */
    @Override
    public void onMessage(List<ConsumerRecord<String, String>> consumerRecords, @NotNull Acknowledgment acknowledgment) {
        final var kafkaNackSleep = Duration.ofMillis(5000);

        for (int i = 0; i < consumerRecords.size(); i++) {
            var consumerRecord = consumerRecords.get(i);
            var task = newPublishedMessageTaskWithTrace(consumerRecord);
            try {
                var result = task.call();
                if (!result.isSuccessful()) {
                    acknowledgment.nack(i, kafkaNackSleep);
                }
            } catch (Exception e) {
                log.error("Unexpected error processing event task", e);
                throw new RuntimeException(e);
            }
        }

        acknowledgment.acknowledge();
    }

    /**
     * Returns a Callable task wrapped with the current trace context obtained from a consumer record.
     *
     * @param consumerRecord the consumer record used to create a task
     * @return a Callable task for processing the received message
     */
    @SuppressWarnings("unchecked")
    private Callable<PublishedMessageTaskResult> newPublishedMessageTaskWithTrace(ConsumerRecord<String, String> consumerRecord) {
        return (Callable<PublishedMessageTaskResult>) tracer.withCurrentTraceContext(publishedMessageTaskFactory.newTask(consumerRecord));
    }

}