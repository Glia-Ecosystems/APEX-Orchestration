package com.netflix.conductor.contribs.kafka;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Singleton;
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
    protected Map<String, KafkaObservableQueue> queues = new ConcurrentHashMap<>();
    private Configuration config;

    /**
     * Initialization of the KafkaEventQueueProvider class
     * @param config Main configuration file for the Conductor application
     * @param injector Google Dependency Injector object that builds the graph of objects for applications
     */
    @Inject
    public KafkaEventQueueProvider(Configuration config, Injector injector){
        this.config = config;
        logger.info("Kafka Event Queue Provider initialized.");
        if (getBoolean("conductor.kafka.listener.enabled")){
            startKafkaListener(injector);
        }
    }

    /**
     * Implements getQueue function for providing a KafkaObservableQueue object with given topic
     * @param queueURI The topic for kafka to subscribe to for consuming and publishing messages
     * @return Initialization of a KafkaObservableQueue object stored in the queue
     */
    @Override
    public ObservableQueue getQueue(String queueURI) {
        return queues.computeIfAbsent(queueURI, q -> new KafkaObservableQueue(queueURI, config));
    }

    /**
     * Starts the process for the Kafka Listener to process client requests to Conductor via Kafka
     * @param injector Google Dependency Injector object that builds the graph of objects for applications
     */
    public void startKafkaListener(Injector injector) {
        String topic = config.getProperty("kafka.topic", "");
        if (topic.isEmpty()) {
            logger.error("Configuration missing for Kafka topic.");
            throw new IllegalArgumentException("Configuration missing for Kafka topic.");
        }
        KafkaObservableQueue kafka = new KafkaObservableQueue(topic, config, injector);
        kafka.listen();
        //Thread kafkaListener = new Thread(new KafkaObservableQueue(topic, config, injector));
        //kafkaListener.setDaemon(true);
        //kafkaListener.start();
        logger.info("Kafka Listener Started.");
    }


}
