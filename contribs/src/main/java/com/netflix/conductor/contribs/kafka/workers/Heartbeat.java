package com.netflix.conductor.contribs.kafka.workers;

import com.google.gson.Gson;
import com.netflix.conductor.contribs.kafka.config.KafkaPropertiesProvider;
import com.netflix.conductor.core.execution.ApplicationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;

public class Heartbeat extends TimerTask {

    private final Logger logger = LoggerFactory.getLogger(Heartbeat.class);
    private static final String HEARTBEAT_TOPIC = "Heartbeat";
    private static final Gson gson = new Gson();
    private final KafkaProducer<String, String> producer;

    public Heartbeat(final KafkaPropertiesProvider kafkaPropertiesProvider){
        this.producer = new KafkaProducer<>(kafkaPropertiesProvider.getProducerProperties());
    }

    /**
     * Create a heartbeat message to be sent
     * @return Heartbeat message as a string
     */
    private String getHeartbeat(){
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
            final ProducerRecord<String, String> record = new ProducerRecord<>(HEARTBEAT_TOPIC, "Orchestrator",
                    getHeartbeat());
            metadata = producer.send(record).get();
            final String producerLogging = "Producer Record: key " + record.key() + ", value " + record.value() +
                    ", partition " + metadata.partition() + ", offset " + metadata.offset();
            logger.debug(producerLogging);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Publish heartbeat to kafka topic {} failed with an error: {}", HEARTBEAT_TOPIC, e.getMessage(), e);
        } catch (final ExecutionException e) {
            logger.error("Publish heartbeat to kafka topic {} failed with an error: {}", HEARTBEAT_TOPIC, e.getMessage(), e);
            throw new ApplicationException(ApplicationException.Code.INTERNAL_ERROR, "Failed to publish the heartbeat");
        }

    }

    @Override
    public void run() {
        sendHeartbeat();
    }
}
