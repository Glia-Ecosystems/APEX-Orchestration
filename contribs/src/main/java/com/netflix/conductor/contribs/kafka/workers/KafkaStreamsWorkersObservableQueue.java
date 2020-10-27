package com.netflix.conductor.contribs.kafka.workers;

import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.contribs.kafka.config.KafkaPropertiesProvider;
import com.netflix.conductor.contribs.kafka.model.RequestContainer;
import com.netflix.conductor.contribs.kafka.model.ResponseContainer;
import com.netflix.conductor.contribs.kafka.resource.handlers.ResourceHandler;
import com.netflix.conductor.contribs.kafka.streamsutil.*;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import javax.inject.Inject;
import java.util.*;

public class KafkaStreamsWorkersObservableQueue implements ObservableQueue, Runnable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsWorkersObservableQueue.class);
    private static final int KEY_ERROR_BRANCH = 0;
    private static final int REGISTER_BRANCH = 1;
    private static final int VALUE_ERROR_BRANCH = 2;
    private static final int REGISTRATION_SUCCESS_BRANCH = 0;
    private static final int REGISTRATION_UNSUCCESSFUL_BRANCH = 1;
    // Create custom Serde objects for processing records
    private final RequestContainerSerde requestContainerSerde;
    private final ResponseContainerSerde responseContainerSerde;
    private final ResourceHandler resourceHandler;
    private final Properties streamsProperties;
    private final String registerWorkersConsumerTopic;
    private final String registerWorkersProducerTopic;
    private final HeartbeatCoordinator heartbeatCoordinator;
    private final WorkersTaskStreamFactory workersTaskStreamFactory;

    @Inject
    public KafkaStreamsWorkersObservableQueue(final ResourceHandler resourceHandler, final Configuration configuration,
                                              final KafkaPropertiesProvider kafkaPropertiesProvider,
                                              final String registerWorkersConsumerTopic,
                                              final String registerWorkersProducerTopic) {
        this.resourceHandler = resourceHandler;
        this.requestContainerSerde = new RequestContainerSerde(new JsonMapperProvider().get());
        this.responseContainerSerde = new ResponseContainerSerde();
        this.registerWorkersConsumerTopic = registerWorkersConsumerTopic;
        this.registerWorkersProducerTopic = registerWorkersProducerTopic;
        this.streamsProperties = kafkaPropertiesProvider.getStreamsProperties("worker-register");
        this.heartbeatCoordinator = new HeartbeatCoordinator(configuration, kafkaPropertiesProvider);
        this.workersTaskStreamFactory = new WorkersTaskStreamFactory(configuration, kafkaPropertiesProvider,
                resourceHandler, new JsonMapperProvider().get());
    }

    /**
     * Creates the topology for registering workers to Conductor and creating task streams object for
     * the registered worker to process tasks between worker and Conductor
     *
     *
     * @return A kafka task streams topology for registering workers
     */
    @SuppressWarnings("unchecked")
    private Topology buildWorkersRegistrationTopology(){
        logger.info("Building Kafka Streams Topology for handling registration of workers to Conductor");
        // Build kafka streams topology
        StreamsBuilder builder = new StreamsBuilder();
        // Parent Node
        // Source Node (Responsible for consuming the records from a given topic, that will be processed)
        KStream<String, RequestContainer> registerStream = builder.stream(registerWorkersConsumerTopic,
                Consumed.with(Serdes.String(), requestContainerSerde))
                .peek((k, v) -> logger.info("Worker {} requesting registration to Conductor: {}", k, v));
        // Branch Processor Node
        // Each record is matched against the given predicates in the order that they're provided.
        // The branch processor will assign records to a stream on the first match.
        // WARNING: No attempts are made to match additional predicates.
        // If a no key given error occurs, send error to client who made initial request
        // Filters processing of request, if any key or value errors occur when containerising request
        // send error to service, else process request
        Predicate<String, RequestContainer> keyError = (serviceName, request) -> serviceName.isEmpty();
        Predicate<String, RequestContainer> readyToRegister = (serviceName, request) -> !request.isDeserializationErrorOccurred();
        Predicate<String, RequestContainer> errorOccurred = (serviceName, request) -> request.isDeserializationErrorOccurred();
        KStream<String, RequestContainer>[] executeDept = registerStream.branch(keyError, readyToRegister, errorOccurred);
        KStream<String, ResponseContainer> processedKeyError = executeDept[KEY_ERROR_BRANCH]
                .mapValues(KafkaStreamsDeserializationExceptionHandler::processKeyError);
        KStream<String, ResponseContainer> processedRegistrationOfWorker = executeDept[REGISTER_BRANCH]
                .mapValues(resourceHandler::processRequest);
        KStream<String, ResponseContainer> processedValueError = executeDept[VALUE_ERROR_BRANCH]
                .mapValues(KafkaStreamsDeserializationExceptionHandler::processValueError);
        // If key or value error occurred, return error to service
        processedKeyError.to(registerWorkersProducerTopic, Produced.with(Serdes.String(), responseContainerSerde));
        processedValueError.to(registerWorkersProducerTopic, Produced.with(Serdes.String(), responseContainerSerde));
        // Branch Processor Node
        // Filters the response from Conductor relating to the registration of the service task definition requested.
        // If success, start worker task stream and return kafka topics for processing tasks
        // If unsuccessful, just return response to service
        Predicate<String, ResponseContainer> registrationSuccessful = (serviceName, response) -> response.getStatus() == 200;
        Predicate<String, ResponseContainer> registrationUnSuccessful = (serviceName, response) -> response.getStatus() != 200;
        KStream<String, ResponseContainer>[] successDept = processedRegistrationOfWorker.branch(registrationSuccessful,
                registrationUnSuccessful);
        successDept[REGISTRATION_SUCCESS_BRANCH].mapValues(workersTaskStreamFactory::createWorkerTaskStream)
                .to(registerWorkersProducerTopic, Produced.with(Serdes.String(), responseContainerSerde));
        successDept[REGISTRATION_UNSUCCESSFUL_BRANCH].to(registerWorkersProducerTopic, Produced.with(Serdes.String(),
                responseContainerSerde));
        return builder.build();
    }

    /**
     * Create a KafkaStreams object containing the built topology and properties file
     * for processing requests to the Conductor API via kafka streams.
     *
     * @param streamsTopology A topology object containing the structure of the kafka streams
     */
    private KafkaStreams buildKafkaStream(Topology streamsTopology) {
        KafkaStreams builtStream = new KafkaStreams(streamsTopology, streamsProperties);
        // here you should examine the throwable/exception and perform an appropriate action!

        // Note on exception handling.
        // Exception handling can be implemented via implementing interfaces such as
        // ProductionExceptionHandler or overriding/extending classes such as LogAndContinueExceptionHandler
        // for deserialization exceptions.
        // ProductionExceptionHandler handles only exceptions on producer (exceptions occurring when sending messages
        // via producer), it will not handle exceptions during processing of stream methods (mapValues(), branch(), etc.)
        // You will need to wrap these methods in try / catch blocks.
        // For consumer side, kafka streams automatically retry consuming record, because offset will not be changed
        // until record is consumed and processed. Use setUncaughtExceptionHandler to log exception
        // or send message to a failure topic.
        builtStream.setUncaughtExceptionHandler((Thread thread, Throwable throwable) ->
                // You can make it restart the stream, but you have to make sure that this thread is
                // destroyed once a new thread is spawned to restart the kafka streams
                logger.error(String.valueOf(throwable)));
        return builtStream;
    }

    /**
     * Builds and start the kafka stream topology and kafka stream object for registering workers
     * and creating task streams for processing tasks between workers and Conductor API.
     */
    public void startRegisterWorkerStream() {
        // Build the topology
        Topology registerWorkersTopology = buildWorkersRegistrationTopology();
        logger.debug("Register Workers Topology Description: {}", registerWorkersTopology.describe());
        // Build/Create Kafka Streams object for starting and processing via kafka streams
        KafkaStreams registerWorkerStream = buildKafkaStream(registerWorkersTopology);
        // Sleep Thread to make sure the server is up before processing requests to Conductor
        sleepThread();
        // Start stream
        logger.info("Starting Kafka Streams for registering workers to Conductor");
        registerWorkerStream.start();
        // Add shutdown hook to respond to SIGTERM and gracefully close the Streams application.
        Runtime.getRuntime().addShutdownHook(new Thread(registerWorkerStream::close));
    }

    /**
     * Execute a Thread sleep for the established time
     */
    private void sleepThread(){
        // Thread.sleep function is executed so that the kafka stream processing of requests are not sent
        // to Conductor before the server is started
        try {
            Thread.sleep(45000); // 45 secs thread sleep
        } catch (final InterruptedException e) {
            // Restores the interrupt by the InterruptedException so that caller can see that
            // interrupt has occurred.
            Thread.currentThread().interrupt();
            logger.error("Error occurred while trying to sleep Thread. {}", e.getMessage());
        }
    }

    /**
     * Provide RX Observable object for consuming messages from Kafka Consumer
     * @return Observable object
     */
    @Override
    public Observable<Message> observe() {
        return null;
    }

    /**
     * Get type of queue
     * @return Type of queue
     */
    @Override
    public String getType() {
        return null;
    }

    /**
     * Get name of the queue name/ topic
     * @return Queue name/ Topic
     */
    @Override
    public String getName() {
        return null;
    }

    /**
     * Get URI of queue.
     * @return Queue Name/ Topic
     */
    @Override
    public String getURI() {
        return null;
    }

    /**
     * Used to acknowledge Kafka Consumer that the message at the current offset was consumed by subscriber
     *
     * @param messages messages to be ack'ed
     * @return Empty List: An empty list is returned due to this method be an implementation of the ObservableQueue interface
     */
    @Override
    public List<String> ack(List<Message> messages) {
        // This function have not been implemented yet
        logger.error("Called a function not implemented yet.");
        // Restores the interrupt by the InterruptedException so that caller can see that
        // interrupt has occurred.
        Thread.currentThread().interrupt();
        throw new UnsupportedOperationException();
    }

    /**
     * Publish message to provided  topic
     *
     * @param messages Messages to be published
     */
    @Override
    public void publish(List<Message> messages) {
        // This function have not been implemented yet
        logger.error("Called a function not implemented yet.");
        // Restores the interrupt by the InterruptedException so that caller can see that
        // interrupt has occurred.
        Thread.currentThread().interrupt();
        throw new UnsupportedOperationException();
    }

    /**
     * Extends the lease of the unacknowledged message consumed from provided topic for a longer duration
     *
     * @param message      Message for which the timeout has to be changed
     * @param unackTimeout timeout in milliseconds for which the unack lease should be extended. (replaces the current value with this value)
     */
    @Override
    public void setUnackTimeout(Message message, long unackTimeout) {
        // This function have not been implemented yet
        logger.error("Called a function not implemented yet.");
        // Restores the interrupt by the InterruptedException so that caller can see that
        // interrupt has occurred.
        Thread.currentThread().interrupt();
        throw new UnsupportedOperationException();
    }

    /**
     * Size of the queue
     * @return size
     */
    @Override
    public long size() {
        return 0;
    }

    /**
     * Clean stop of the heartbeat thread
     */
    private void cleanUp(){
        heartbeatCoordinator.stopHeartbeat();
    }

    /**
     * Creates a separate thread from the main thread for using Kafka Streams to
     * register workers and processing tasks between workers and Conductor API
     */
    @Override
    public void run() {
        try {
            startRegisterWorkerStream();
        } catch (final Exception e) {
            logger.error("KafkaStreamsWorkersObservableQueue.startStream(), exiting due to error! %s", e.getCause());
        } finally {
            cleanUp();
        }
    }
}
