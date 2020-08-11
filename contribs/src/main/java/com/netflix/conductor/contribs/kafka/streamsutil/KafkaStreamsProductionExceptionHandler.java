package com.netflix.conductor.contribs.kafka.streamsutil;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KafkaStreamsProductionExceptionHandler implements ProductionExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsProductionExceptionHandler.class);

    /**
     * Exception handler for errors that occur during production/sending the response from Conductor to client
     *
     * @param record Record received from Kafka Streams
     * @param exception Exception that occurred during production/sending the response from Conductor to client
     * @return ProductionHandlerResponse object to inform kafka streams to retry sending response to client
     */
    @Override
    public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception exception) {
        String message = new String(record.value());
        String topic = record.topic();
        logger.error("Error while attempting to send message. Message: {}, Destination Topic: {}",
                message, topic, exception);
        return ProductionExceptionHandlerResponse.CONTINUE;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        // Ignore
    }
}
