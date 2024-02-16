// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.utils;

import org.springframework.kafka.KafkaException;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.EmbeddedKafkaKraftBroker;

public final class EmbeddedKafkaHolder {

    private static final EmbeddedKafkaBroker embeddedKafka = new EmbeddedKafkaKraftBroker(1, 1)
            .brokerListProperty("horizon.kafka.bootstrapServers");

    private static boolean isEmbeddedKafkaInitialized;

    public static EmbeddedKafkaBroker getEmbeddedKafka() {
        if (!isEmbeddedKafkaInitialized) {
            try {
                embeddedKafka.afterPropertiesSet();
            }
            catch (Exception e) {
                throw new KafkaException("Embedded broker failed to start", e);
            }
            isEmbeddedKafkaInitialized = true;
        }
        return embeddedKafka;
    }

    private EmbeddedKafkaHolder() {
        super();
    }

}
