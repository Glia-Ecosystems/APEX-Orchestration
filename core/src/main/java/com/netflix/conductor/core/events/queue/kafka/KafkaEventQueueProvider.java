package com.netflix.conductor.core.events.queue.kafka;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.inject.Inject;
import javax.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.EventQueueProvider;
import com.netflix.conductor.core.events.queue.ObservableQueue;

/**
 * @author preeth
 */
@Singleton
public class KafkaEventQueueProvider implements EventQueueProvider {
    private static Logger logger = LoggerFactory.getLogger(KafkaEventQueueProvider.class);
    protected Map<String, KafkaObservableQueue> queues = new ConcurrentHashMap<>();
    private Configuration config;

    @Inject
    public KafkaEventQueueProvider(Configuration config) {
        this.config = config;
        logger.info("Kafka Event Queue Provider initialized.");
    }

    @Override
    public ObservableQueue getQueue(String queueURI) {
        return queues.computeIfAbsent(queueURI, q -> new KafkaObservableQueue(queueURI, config));
    }
}
