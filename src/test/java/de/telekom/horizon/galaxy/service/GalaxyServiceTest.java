package de.telekom.horizon.galaxy.service;

import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class GalaxyServiceTest {

    @Test
    void startsKafkaOnlyAfterApplicationIsReady() {
        @SuppressWarnings("unchecked")
        var container = (ConcurrentMessageListenerContainer<String, String>) mock(ConcurrentMessageListenerContainer.class);
        var service = new GalaxyService(container, mock(ApplicationContext.class));

        verifyNoInteractions(container);
        when(container.isRunning()).thenReturn(false, true);
        service.applicationReadyHandler();
        service.applicationReadyHandler();

        verify(container, times(1)).start();
    }
}