// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "galaxy")
@Getter
@Setter
public class GalaxyConfig {
    private String defaultEnvironment;

    @Value("${horizon.kafka.consumingTopic}")
    private String consumingTopic;

    private boolean featureJsonPathFilteringEnabled;

    @Value("#{'${galaxy.feature-jsonpath-filtering-event-types:}'.split(',')}")
    private List<String> featureJsonPathFilteringEventTypes;
}
