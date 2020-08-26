package com.netflix.conductor.contribs.kafka;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.contribs.kafka.config.KafkaPropertiesProvider;
import com.netflix.conductor.contribs.kafka.resource.handlers.ResourceHandler;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.EventQueueProvider;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import static java.lang.Boolean.getBoolean;

/**
 * KafkaEventQueueProvider class provides event queue kafka objects as well as the ability
 * to initialize Kafka for processing client Conductor API requests
 *
 * @author Glia Ecosystems
 */
@Singleton
public class KafkaEventQueueProvider implements EventQueueProvider {
    private static final Logger logger = LoggerFactory.getLogger(KafkaEventQueueProvider.class);
    protected final Map<String, KafkaObservableQueue> queues = new ConcurrentHashMap<>();
    private final KafkaPropertiesProvider kafkaPropertiesProvider;
    private final Configuration config;

    /**
     * Initialization of the KafkaEventQueueProvider class
     *
     * @param config   Main configuration file for the Conductor application
     * @param injector Google Dependency Injector object that builds the graph of objects for applications
     */
    @Inject
    public KafkaEventQueueProvider(final Configuration config, final Injector injector) {
        this.config = config;
        this.kafkaPropertiesProvider = new KafkaPropertiesProvider(config);
        logger.info("Kafka Event Queue Provider initialized.");
        // Placeholder for resource handler class if needed to be initialized
        ResourceHandler resourceHandler = null;
        if (getBoolean("conductor.kafka.listener.enabled")) {
            resourceHandler = new ResourceHandler(injector, new JsonMapperProvider().get());
            startKafkaListener(resourceHandler);
        }
        if (getBoolean("conductor.kafka.workers.listener.enabled")){
            if (resourceHandler == null){
                resourceHandler = new ResourceHandler(injector, new JsonMapperProvider().get());
            }
            startKafkaWorkersListener(resourceHandler);
        }
    }

    /**
     * Implements getQueue function for providing a KafkaObservableQueue object with given topic
     *
     * @param queueURI The topic for kafka to subscribe to for consuming and publishing messages
     * @return Initialization of a KafkaObservableQueue object stored in the queue
     */
    @Override
    public ObservableQueue getQueue(final String queueURI) {
        return queues.computeIfAbsent(queueURI, q -> new KafkaObservableQueue(queueURI, config,
                kafkaPropertiesProvider));
    }

    /**
     * Starts the process for the Kafka Listener to process client requests to Conductor via Kafka
     *
     * @param resourceHandler  Main class for accessing resource classes of Conductor
     */
    public void startKafkaListener(final ResourceHandler resourceHandler) {
        final String consumerTopic = config.getProperty("kafka.consumer.listener.topic", "");
        final String producerTopic = config.getProperty("kafka.producer.listener.topic", "");
        if (consumerTopic.isEmpty() && producerTopic.isEmpty()) {
            logger.error("Configuration missing for Kafka Consumer and/or Producer topics.");
            throw new IllegalArgumentException("Configuration missing for Kafka Consumer and/or Producer topics.");
        }
        Thread kafkaListener = new Thread(new KafkaStreamsObservableQueue(resourceHandler, config,
                                                                          kafkaPropertiesProvider,
                                                                          consumerTopic,
                                                                          producerTopic));
        kafkaListener.setDaemon(true);
        kafkaListener.start();
        logger.info("Kafka Listener Started.");
    }

    /**
     *  Starts the process for the Kafka Listener to process tasks between workers/services and Conductor
     *
     * @param resourceHandler Main class for accessing resource classes of Conductor
     */
    public void startKafkaWorkersListener(final ResourceHandler resourceHandler){
        final String registerWorkersConsumerTopic = config.getProperty("kafka.streams.workers.consumer.listener.topic", "");
        final String registerWorkersProducerTopic = config.getProperty("kafka.streams.workers.producer.listener.topic", "");
        if (registerWorkersConsumerTopic.isEmpty() && registerWorkersProducerTopic.isEmpty()) {
            logger.error("Topic missing for Kafka Worker listener.");
            throw new IllegalArgumentException("Topic missing for Kafka Worker listener.");
        }
        Thread kafkaWorkerListener = new Thread(new KafkaStreamsWorkersObservableQueue(resourceHandler, config,
                                                                                       kafkaPropertiesProvider,
                                                                                       registerWorkersConsumerTopic,
                                                                                       registerWorkersProducerTopic));
        kafkaWorkerListener.setDaemon(true);
        kafkaWorkerListener.start();
        logger.info("Kafka Workers Listener Started.");
    }


}
