package com.netflix.conductor.contribs.kafka.streamsutil;

import com.netflix.conductor.core.config.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.errors.ProductionExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class KafkaStreamsProductionExceptionHandler  {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsProductionExceptionHandler.class);
    private static KafkaProducer<String, String> producer;
    private static ExecutorService threadPool;
    private static int retryCount;
    private static Long retryDelayMilliSeconds;

    public KafkaStreamsProductionExceptionHandler(final Configuration configuration, final Properties producerProperties){
        setRetryCount(configuration.getIntProperty("conductor.kafka.streams.production.exception.handler.retry.count", 3));
        setRetryDelayMilliSeconds( configuration.getLongProperty("conductor.kafka.streams.production.exception.handler.retry.delay.ms", 5000));
        setThreadPool(Executors.newFixedThreadPool(configuration.getIntProperty("conductor.kafka.streams.production.exception.handler.thread.pool", 10)));
        setProducer(new KafkaProducer<>(producerProperties));
        cleanUp();
    }

    /**
     *
     * @param topic
     * @param recordKey
     * @param recordMessage
     */
    public static void retry(String topic, String recordKey, String recordMessage){
        threadPool.execute(() -> {
            try {
                reattemptsPublishMessage(topic, recordKey, recordMessage);
            } catch (InterruptedException e) {
                // Restores the interrupt by the InterruptedException so that caller can see that
                // interrupt has occurred.
                Thread.currentThread().interrupt();
                logger.error("Error occurred while trying resend record during production exception handling. {}",
                        e.getMessage());
            }
        });
    }

    /**
     *
     * @param topic
     * @param key
     * @param message
     * @throws InterruptedException
     */
    private static void reattemptsPublishMessage(String topic, String key, String message) throws InterruptedException {
        Long exponentialDelay = retryDelayMilliSeconds;
        for (int i = 0; i < retryCount; i++){
            boolean successful = sendMessageAttempt(topic, key, message);
            if (!successful){
                Thread.sleep(exponentialDelay);
                exponentialDelay = exponentialDelay * exponentialDelay;
            }
        }
    }

    /**
     *
     * @param topic
     * @param key
     * @param message
     * @return
     * @throws InterruptedException
     */
    private static boolean sendMessageAttempt(final String topic, final String key, final String message) throws InterruptedException {
        try {
            producer.send(new ProducerRecord<>(topic, key, message)).get();
            logger.debug("Successfully reattempted to send message. Message: {}, Destination Topic: {}", message, topic);
            return true;
        } catch (final ExecutionException e) {
            logger.error("Publish task to kafka topic {} failed with an error: {}", topic, e.getMessage(), e);
            return false;
        }
    }

    /**
     *
     * @return
     */
    public Class<ExceptionHandlerProduction> getProductionExceptionHandler(){
        return ExceptionHandlerProduction.class;
    }

    /**
     *
     * @param retryCount
     */
    private static void setRetryCount(int retryCount) {
        KafkaStreamsProductionExceptionHandler.retryCount = retryCount;
    }

    /**
     *
     * @param producer
     */
    private static void setProducer(KafkaProducer<String, String> producer) {
        KafkaStreamsProductionExceptionHandler.producer = producer;
    }

    /**
     *
     * @param threadPool
     */
    private static void setThreadPool(ExecutorService threadPool) {
        KafkaStreamsProductionExceptionHandler.threadPool = threadPool;
    }

    /**
     *
     * @param retryDelayMilliSeconds
     */
    private static void setRetryDelayMilliSeconds(Long retryDelayMilliSeconds) {
        KafkaStreamsProductionExceptionHandler.retryDelayMilliSeconds = retryDelayMilliSeconds;
    }

    /**
     *
     */
    private void cleanUp(){
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));
    }

    /**
     *
     */
    public static class ExceptionHandlerProduction implements ProductionExceptionHandler{

        /**
         * Exception handler for errors that occur during production/sending the response from Conductor to client
         *
         * @param record Record received from Kafka Streams
         * @param exception Exception that occurred during production/sending the response from Conductor to client
         * @return ProductionHandlerResponse object to inform kafka streams to retry sending response to client
         */
        @Override
        public ProductionExceptionHandlerResponse handle(ProducerRecord<byte[], byte[]> record, Exception exception) {
            String topic = record.topic();
            String recordMessage = new String(record.value());
            logger.error("Error while attempting to send message. Message: {}, Destination Topic: {}",
                    recordMessage, topic, exception);
            retry(topic, new String(record.key()), recordMessage);
            return ProductionExceptionHandlerResponse.CONTINUE;
        }

        @Override
        public void configure(Map<String, ?> configs) {
            // Ignore
        }
    }
}
