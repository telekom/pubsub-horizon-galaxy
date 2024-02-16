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
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * The {@code PublishedMessageListener} class is responsible for processing Kafka messages in batches.
 * <p>
 * The class handles new {@link ConsumerRecord} objects containing {@link PublishedEventMessage} objects.
 * For each {@link ConsumerRecord} a task is created using the {@link PublishedMessageTaskFactory}.
 * The result of each task is collected in a list of {@link Future} objects.
 * If any task in the list fails, a nack (negative acknowledgment) for the failed message
 * and all following messages in the batch is sent to Kafka. All messages before the failed message are getting acknowledged.
 * If all tasks are successful, an acknowledgment for the batch is sent to Kafka.
 */
@Slf4j
public class PublishedMessageListener extends AbstractConsumerSeekAware implements BatchAcknowledgingMessageListener<String, String> {

    private final PublishedMessageTaskFactory publishedMessageTaskFactory;
    private final ThreadPoolTaskExecutor taskExecutor;
    private final HorizonTracer tracer;


    public PublishedMessageListener(PublishedMessageTaskFactory publishedMessageTaskFactory, HorizonTracer horizonTracer, GalaxyConfig galaxyConfig) {
        super();
        this.publishedMessageTaskFactory = publishedMessageTaskFactory;
        this.tracer = horizonTracer;

        this.taskExecutor = initThreadPoolTaskExecutor(galaxyConfig);
    }

    @NotNull
    private ThreadPoolTaskExecutor initThreadPoolTaskExecutor(GalaxyConfig galaxyConfig) {
        final ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setCorePoolSize(galaxyConfig.getBatchCoreThreadPoolSize());
        threadPoolTaskExecutor.setMaxPoolSize(galaxyConfig.getBatchMaxThreadPoolSize());
        threadPoolTaskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        threadPoolTaskExecutor.setPrestartAllCoreThreads(true);
        threadPoolTaskExecutor.setThreadGroupName("batch");
        threadPoolTaskExecutor.setThreadNamePrefix("batch");
        threadPoolTaskExecutor.afterPropertiesSet();
        return threadPoolTaskExecutor;
    }

    /**
     * Handles a batch of messages received from Kafka.
     *
     * @param consumerRecords the records received from Kafka
     * @param acknowledgment  the acknowledgment object used to nack or ack the batch (partially)
     */
    @Override
    public void onMessage(List<ConsumerRecord<String, String>> consumerRecords, @NotNull Acknowledgment acknowledgment) {
        List<Future<PublishedMessageTaskResult>> taskFutureList = new ArrayList<>();

        for (ConsumerRecord<String, String> consumerRecord: consumerRecords) {
            var task = getPublishedMessageTaskResultCallable(consumerRecord);
            Future<PublishedMessageTaskResult> taskFuture = taskExecutor.submit(task);
            taskFutureList.add(taskFuture);
        }

        var nackIndex = -1;
        for (int index = 0; index < taskFutureList.size(); index++) {
            try {
                var taskResult = taskFutureList.get(index).get();
                if (!taskResult.isSuccessful() && nackIndex == -1) {
                    nackIndex = index;
                }
            } catch (ExecutionException | InterruptedException e) {
                log.error("Unexpected error processing event task", e);
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

        //If a nackIndex has been set during the processing of the futures, a nack will be sent to Kafka
        if (nackIndex < 0) {
            acknowledgment.acknowledge();
        } else {
            acknowledgment.nack(nackIndex, Duration.ofMillis(5000));
        }

    }

    /**
     * Returns a Callable task wrapped with the current trace context obtained from a consumer record.
     *
     * @param consumerRecord the consumer record used to create a task
     * @return a Callable task for processing the received message
     */
    @SuppressWarnings("unchecked")
    private Callable<PublishedMessageTaskResult> getPublishedMessageTaskResultCallable(ConsumerRecord<String, String> consumerRecord) {
        return (Callable<PublishedMessageTaskResult>) tracer.withCurrentTraceContext(publishedMessageTaskFactory.newTask(consumerRecord));
    }

}