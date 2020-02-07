package com.netflix.conductor.contribs.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoMap;
import com.google.inject.multibindings.StringMapKey;
import com.google.inject.name.Named;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.EventQueueProvider;
import com.netflix.conductor.core.events.queue.kafka.KafkaEventQueueProvider;

/**
 * @author preeth
 *
 */
public class KafkaConsumer extends AbstractModule {
    private static Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    @Override
    protected void configure() {
        logger.info("Kafka module KafkaConsumer.");
    }

    @ProvidesIntoMap
    @StringMapKey("kafka")
    @Singleton
    @Named("EventQueueProviders")

    public EventQueueProvider getKafkaEventQueueProvider(Configuration configuration) {
        return new KafkaEventQueueProvider(configuration);
    }

}