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
     *  Using a ThreadPool, spawn a separate thread to reattempt publish of message to respective topic
     *
     * @param topic The kafka topic where the message should be publish
     * @param recordKey  The key of the message
     * @param recordMessage The message to be resend
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
     * Reattempt publish of message with certain number of reattempts and delays between each attempt
     *
     * @param topic The kafka topic where the message should be publish
     * @param key The key of the message
     * @param message The message to be resend
     * @throws InterruptedException Occurs when a thread is interrupted before or during its respective activity
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
     *  Publish message to topic
     *
     * @param topic The kafka topic where the message should be publish
     * @param key The key of the message
     * @param message The message to be resend
     * @return Indicator if the message was successfully resented
     * @throws InterruptedException Occurs when a thread is interrupted before or during its respective activity
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
     * Get inner class object
     *
     * @return Inner (ExceptionHandlerProduction) class object
     */
    public Class<ExceptionHandlerProduction> getProductionExceptionHandler(){
        return ExceptionHandlerProduction.class;
    }

    /**
     * Set retry count variable
     *
     * This setter method is used to suppress warning for not assigning static class variable upon instantiation of class
     *
     * @param retryCount The number of retries to be attempted in resending the message
     */
    private static void setRetryCount(int retryCount) {
        KafkaStreamsProductionExceptionHandler.retryCount = retryCount;
    }

    /**
     * Set producer variable
     *
     * This setter method is used to suppress warning for not assigning static class variable upon instantiation of class
     *
     * @param producer A Kafka Producer object
     */
    private static void setProducer(KafkaProducer<String, String> producer) {
        KafkaStreamsProductionExceptionHandler.producer = producer;
    }

    /**
     * Set thread pool variable
     *
     * This setter method is used to suppress warning for not assigning static class variable upon instantiation of class
     *
     * @param threadPool A Thread pool for reusing a capped specified number of threads for attempts to resend messages
     */
    private static void setThreadPool(ExecutorService threadPool) {
        KafkaStreamsProductionExceptionHandler.threadPool = threadPool;
    }

    /**
     * Set retry delay milliseconds variable
     *
     * This setter method is used to suppress warning for not assigning static class variable upon instantiation of class
     *
     * @param retryDelayMilliSeconds The time in milliseconds, indicating intervals between reattempts of sending messages
     */
    private static void setRetryDelayMilliSeconds(Long retryDelayMilliSeconds) {
        KafkaStreamsProductionExceptionHandler.retryDelayMilliSeconds = retryDelayMilliSeconds;
    }

    /**
     * Creates a shutdown hook to close the Kafka producer upon closing of the application
     */
    private void cleanUp(){
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));
    }

    /**
     * A Custom Production Exception Handler for Kafka Streams
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
