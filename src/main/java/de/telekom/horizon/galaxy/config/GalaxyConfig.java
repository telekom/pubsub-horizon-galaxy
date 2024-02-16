package de.telekom.horizon.galaxy.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "galaxy")
@Getter
@Setter
public class GalaxyConfig {

    private int batchCoreThreadPoolSize;

    private int batchMaxThreadPoolSize;

    private int subscriptionCoreThreadPoolSize;

    private int subscriptionMaxThreadPoolSize;

    private String defaultEnvironment;

    @Value("${horizon.kafka.consumingTopic}")
    private String consumingTopic;
}
