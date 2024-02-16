// Copyright 2024 Deutsche Telekom IT GmbH
//
// SPDX-License-Identifier: Apache-2.0

package de.telekom.horizon.galaxy.service;

import de.telekom.horizon.galaxy.utils.AbstractIntegrationTest;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNull;

class PublishedInvalidJsonAckTest extends AbstractIntegrationTest {


    @Value("${horizon.kafka.consumingTopic}")
    private String consumingTopic;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void shouldAckInvalidPublishedMessage() throws InterruptedException {
        // given
        String iAmInvalid = """
                {
                    "foo":
                """;

        // when
        kafkaTemplate.send(consumingTopic, iAmInvalid);

        // then
        ConsumerRecord<String, String> received = pollForRecord(5, TimeUnit.SECONDS);
        assertNull(received);
    }

}
