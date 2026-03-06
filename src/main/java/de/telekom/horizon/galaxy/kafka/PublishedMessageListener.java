// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.kafka;

import de.telekom.eni.pandora.horizon.model.event.PublishedEventMessage;
import de.telekom.eni.pandora.horizon.tracing.HorizonTracer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.jetbrains.annotations.NotNull;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

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
    private static final Duration KAFKA_NACK_SLEEP = Duration.ofMillis(5000);
    private static final int NO_NACK_INDEX = -1;

    private final PublishedMessageTaskFactory publishedMessageTaskFactory;
    private final HorizonTracer tracer;

    public PublishedMessageListener(PublishedMessageTaskFactory publishedMessageTaskFactory, HorizonTracer horizonTracer) {
        super();
        this.publishedMessageTaskFactory = publishedMessageTaskFactory;
        this.tracer = horizonTracer;

    }

    /**
     * Handles a batch of messages received from Kafka.
     * The processing logic is as follows:
     * <ul>
     *     <li> Messages are sequentially processed, multiplexed and are submitted to the {@code EventWriter} </li>
     *     <li> The {@code EventWriter} asynchronously manages the message publishing, returning a {@code CompletableFuture} upon message
     *     submission. The publishing of the multiplexed messages is hence <b>asynchronous</b>. </li>
     *     <li> The {@code CompletableFuture}s returned by {@code EventWriter}  are collected into a single {@code CompletableFuture} to observe
     *     the status of the publishing on the <b>ConsumerRecord</b> granularity-level. </li>
     *     <li> An {@code .exceptionally} callback is registered to track the least index of the failed message inside the {@code ConsumerRecords} batch. </li>
     *     <li> Message processing is stopped upon first encountered error. As there is no known mechanism to propagate
     *     the cancellation to the {@code EventWriter}, we wait for the messages submitted for publishing to complete and
     *     proceed with {@code nack}-ing the batch from the least failedIndex value </li>
     * </ul>
     *
     * @param consumerRecords the records received from Kafka
     * @param acknowledgment  the acknowledgment object used to nack or ack the batch (partially)
     */
    @Override
    public void onMessage(List<ConsumerRecord<String, String>> consumerRecords, @NotNull Acknowledgment acknowledgment) {
        final var failedIndex = new AtomicInteger(NO_NACK_INDEX);
        final var messagePublishingStatuses = new ArrayList<CompletableFuture<Void>>(consumerRecords.size());

        for (int i = 0; i < consumerRecords.size(); i++) {
            if (failedIndex.get() != NO_NACK_INDEX) {
                break;
            }

            final var consumerRecord = consumerRecords.get(i);
            final var task = newPublishedMessageTaskWithTrace(consumerRecord);
            final var messageInBatchIndex = i;
            try {
                messagePublishingStatuses.add(task
                        .call()
                        .exceptionally(ex -> {
                            failedIndex.getAndUpdate(oldValue -> {
                                if (oldValue == -1) {
                                    return messageInBatchIndex;
                                }
                                return Math.min(messageInBatchIndex, oldValue);
                            });
                            return null;
                        }));
            } catch (Exception e) {
                log.error("Unexpected error processing event task", e);
                throw new RuntimeException(e);
            }
        }

        try {
            CompletableFuture
                    .allOf(messagePublishingStatuses.toArray(new CompletableFuture[0]))
                    .get();

            final var failedIndexValue = failedIndex.get();
            if (failedIndexValue != NO_NACK_INDEX) {
                acknowledgment.nack(failedIndexValue, KAFKA_NACK_SLEEP);
                return;
            }
            acknowledgment.acknowledge();
        } catch (InterruptedException e) {
            log.error("Interrupted while waiting for message publishing", e);
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns a Callable task wrapped with the current trace context obtained from a consumer record.
     *
     * @param consumerRecord the consumer record used to create a task
     * @return a Callable task for processing the received message
     */
    private Callable<CompletableFuture<Void>> newPublishedMessageTaskWithTrace(ConsumerRecord<String, String> consumerRecord) {
        return tracer.withCurrentContext(publishedMessageTaskFactory.newTask(consumerRecord));
    }

}