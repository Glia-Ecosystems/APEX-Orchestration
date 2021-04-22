package com.netflix.conductor.contribs.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.contribs.kafka.config.KafkaProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;


/**
 * KafkaStreamsProvider class provides kafka stream objects for processing client Conductor API requests,
 * registering services task definition to Conductor, and processing tasks between services and
 * Conductor
 *
 * @author Glia Ecosystems
 */
@Component
@ConditionalOnProperty(name = "conductor.kafka.additional.component", havingValue = "true")
public class KafkaStreamsProvider {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsProvider.class);
    private final ApplicationContext context;
    private final KafkaProperties config;
    private final ObjectMapper objectMapper;
    @Value("${conductor.kafka.additional.component}")
    private boolean kafkaListenerEnabled;
    @Value("${conductor.kafka.workers.listener.enabled}")
    private boolean kafkaWorkerListenerEnabled;

    public KafkaStreamsProvider(final KafkaProperties config, final ApplicationContext context,
                                final ObjectMapper objectMapper) {
        this.config = config;
        this.context = context;
        this.objectMapper = objectMapper;
    }

    public void startKafkaListener(){
        // This function have not been implemented yet
        logger.error("Called a function not implemented yet.");
    }

    public void startKafkaWorkerListener(){
        // This function have not been implemented yet
        logger.error("Called a function not implemented yet.");
    }
}
