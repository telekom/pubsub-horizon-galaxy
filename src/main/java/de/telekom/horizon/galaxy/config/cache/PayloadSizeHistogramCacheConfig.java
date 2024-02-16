package de.telekom.horizon.galaxy.config.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.telekom.eni.pandora.horizon.metrics.HorizonMetricsHelper;
import de.telekom.horizon.galaxy.cache.PayloadSizeHistogramCache;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static de.telekom.eni.pandora.horizon.metrics.HorizonMetricsConstants.METRIC_PAYLOAD_SIZE_INCOMING;
import static de.telekom.eni.pandora.horizon.metrics.HorizonMetricsConstants.METRIC_PAYLOAD_SIZE_OUTGOING;

@Configuration
public class PayloadSizeHistogramCacheConfig {

    @Bean(name = "incomingPayloadSizeCache")
    public PayloadSizeHistogramCache incomingPayloadSizeCache(HorizonMetricsHelper metricsHelper, ObjectMapper objectMapper) {
        return new PayloadSizeHistogramCache(metricsHelper, METRIC_PAYLOAD_SIZE_INCOMING, objectMapper);
    }

    @Bean(name = "outgoingPayloadSizeCache")
    public PayloadSizeHistogramCache outgoingPayloadSizeCache(HorizonMetricsHelper metricsHelper, ObjectMapper objectMapper) {
        return new PayloadSizeHistogramCache(metricsHelper, METRIC_PAYLOAD_SIZE_OUTGOING, objectMapper);
    }

}
