package com.netflix.conductor.contribs.kafka.model;

import com.google.gson.Gson;
import com.netflix.conductor.contribs.kafka.config.KafkaPropertiesProvider;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.execution.ApplicationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Heartbeat extends TimerTask {

    private final Logger logger = LoggerFactory.getLogger(Heartbeat.class);
    private static final Gson gson = new Gson();
    private final KafkaProducer<String, String> producer;
    private final String heartbeatTopic;

    public Heartbeat(final Configuration configuration, final KafkaPropertiesProvider kafkaPropertiesProvider,
                     final KafkaTopicsManager kafkaTopicsManager){
        this.heartbeatTopic = getHeartbeatTopic(configuration, kafkaTopicsManager);
        this.producer = new KafkaProducer<>(kafkaPropertiesProvider.getProducerProperties());
    }

    /**
     * Get heartbeat topic from configuration
     *
     * @param configuration Main configuration file for the Conductor application
     * @return Topic for Conductor to publish heartbeats
     */
    private String getHeartbeatTopic(final Configuration configuration, final KafkaTopicsManager kafkaTopicsManager){
        String topic = configuration.getProperty("heartbeat.topic", "");
        if (topic == null){
            logger.error("Configuration missing heartbeat topic.");
            throw new IllegalArgumentException("Configuration missing heartbeat topic..");
        }
        kafkaTopicsManager.createTopic(topic);
        return topic;
    }

    /**
     * Create a heartbeat message to be sent
     *
     * @return Heartbeat message as a string
     */
    private String createHeartbeat(){
        Map<String, Long> heartbeat = new HashMap<>();
        final long now = System.currentTimeMillis();
        heartbeat.put("HB", now);
        return gson.toJson(heartbeat);
    }

    /**
     * Publish the heartbeat to the heartbeat topic for worker/service.
     */
    private void sendHeartbeat(){
        try {
            final RecordMetadata metadata;
            final ProducerRecord<String, String> record = new ProducerRecord<>(heartbeatTopic, "Orchestrator",
                    createHeartbeat());
            metadata = producer.send(record).get();
            final String producerLogging = "Producer Record: key " + record.key() + ", value " + record.value() +
                    ", partition " + metadata.partition() + ", offset " + metadata.offset();
            logger.debug(producerLogging);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Publish heartbeat to kafka topic {} failed with an error: {}", heartbeatTopic, e.getMessage(), e);
        } catch (final ExecutionException e) {
            logger.error("Publish heartbeat to kafka topic {} failed with an error: {}", heartbeatTopic, e.getMessage(), e);
            throw new ApplicationException(ApplicationException.Code.INTERNAL_ERROR, "Failed to publish the heartbeat");
        }

    }

    /**
     * Main function to create a thread for publish heartbeats
     */
    @Override
    public void run() {
        sendHeartbeat();
    }
}
