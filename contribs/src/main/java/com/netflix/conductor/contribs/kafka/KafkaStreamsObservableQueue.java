package com.netflix.conductor.contribs.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.contribs.kafka.config.KafkaPropertiesProvider;
import com.netflix.conductor.contribs.kafka.model.RequestContainer;
import com.netflix.conductor.contribs.kafka.model.ResponseContainer;
import com.netflix.conductor.contribs.kafka.model.WorkflowStatusMonitor;
import com.netflix.conductor.contribs.kafka.resource.handlers.ResourceHandler;
import com.netflix.conductor.contribs.kafka.streamsutil.KafkaStreamsDeserializationExceptionHandler;
import com.netflix.conductor.contribs.kafka.streamsutil.RequestContainerSerde;
import com.netflix.conductor.contribs.kafka.streamsutil.ResponseContainerSerde;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * This class is the main class for processing client requests to Orchestrator using Kafka Streams
 *
 * Reads the properties with prefix 'kafka.streams.', and 'kafka.' from the
 * provided configuration. Initializes kafka streams based on the given value. Topics are provided
 * from the configuration. It is assumed that the topics provided is already configured in
 * the kafka cluster.
 *
 * Basic steps for kafka streams
 * 1. Define the configurations
 * 2. Create Serde instances, predefined or custom
 * 3. Build the kafka streams processor topology
 * 4. Create and start the KStream
 *
 * @author Glia Ecosystems
 */
public class KafkaStreamsObservableQueue implements ObservableQueue, Runnable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsObservableQueue.class);
    private static final String QUEUE_TYPE = "kafkaStreams";
    // Create custom Serde objects for processing records
    private final RequestContainerSerde requestContainerSerde;
    private final ResponseContainerSerde responseContainerSerde;
    private final ObjectMapper objectMapper;
    private final Properties streamsProperties;
    private final String queueName;
    private final String apexRequestsTopic;
    private final String apexResponsesTopic;
    private final ResourceHandler resourceHandler;
    private final KafkaProducer<String, String> producer;
    private final ExecutorService threadPool;
    private final Long startupTreadSleep;
    private final int workflowStatusPollingInterval;

    /**
     * Constructor of the KafkaStreamsObservableQueue for using kafka streams processing
     * client requests to Conductor API and publishing responses from Conductor API to client
     *
     * @param resourceHandler  Main class for accessing resource classes of Conductor
     * @param configuration Main configuration file for the Conductor application
     * @param requestTopic  Topic for consuming messages
     * @param responseTopic Topic for publishing messages
     */
    @Inject
    public KafkaStreamsObservableQueue(final ResourceHandler resourceHandler, final Configuration configuration,
                                       final KafkaPropertiesProvider kafkaPropertiesProvider,
                                       final String requestTopic, final String responseTopic){
        this.queueName = "";
        this.apexRequestsTopic = requestTopic;
        this.apexResponsesTopic = responseTopic;
        this.resourceHandler = resourceHandler;
        this.startupTreadSleep = configuration.getLongProperty("conductor.kafka.listener.startup.thread.sleep", 50000);
        this.requestContainerSerde = new RequestContainerSerde();
        this.responseContainerSerde = new ResponseContainerSerde();
        this.objectMapper = new JsonMapperProvider().get();
        this.workflowStatusPollingInterval = configuration.getIntProperty("conductor.kafka.listener.workflow.status.monitor.polling.interva", 1);
        this.threadPool = Executors.newFixedThreadPool(configuration.getIntProperty("conductor.kafka.listener.thread.pool", 20));
        this.streamsProperties = kafkaPropertiesProvider.getStreamsProperties("client");
        this.producer = new KafkaProducer<>(kafkaPropertiesProvider.getProducerProperties());
    }

    /**
     * Creates the topology for processing the client initial request to the Conductor API
     *
     * @return A kafka streams topology for processing client requests
     */
    @SuppressWarnings("unchecked")
    private Topology buildStreamTopology(){
        logger.info("Building Kafka Streams Topology for handling requests and responses to/from Conductor");
        // Build kafka streams topology
        final StreamsBuilder builder = new StreamsBuilder();
        // Parent Node
        // Source Node (Responsible for consuming the records from a given topic, that will be processed)
        final KStream<String, RequestContainer> requestStream = builder.stream(apexRequestsTopic,
                Consumed.with(Serdes.String(), requestContainerSerde))
                .peek((k, v) -> logger.info("Received record. Client: {}", v.getKey()));
        // Branch Processor Node
        // Each record is matched against the given predicates in the order that they're provided.
        // The branch processor will assign records to a stream on the first match.
        // WARNING: No attempts are made to match additional predicates.
        // NOTE: Branch Processor Node is a list of predicates that are indexed to form the following process
        // if predicate is true.
        // If a value error occurred, send error to client who made initial request
        final Predicate<String, RequestContainer> isError = (emptyKey, request) -> request.isDeserializationErrorOccurred();
        // If no errors occurred during deserialization, process request further
        final Predicate<String, RequestContainer> readyToProcess = (emptyKey, request) -> !request.isDeserializationErrorOccurred();
        final KStream<String, RequestContainer>[] executeDept = requestStream.branch(isError, readyToProcess);
        // Child Node - Process value error
        final KStream<String, ResponseContainer> processedError = executeDept[0].mapValues(KafkaStreamsDeserializationExceptionHandler::processValueError);
        // Child Node - Execute Request to Conductor API and receive response
        final KStream<String, ResponseContainer> processedRequest = executeDept[1].mapValues(resourceHandler::processRequest);
        // Sink Node - Send Response to client
        processedRequest.to(apexResponsesTopic, Produced.with(Serdes.String(), responseContainerSerde));
        // Sink Node - Send Value Error to client
        processedError.to(apexResponsesTopic, Produced.with(Serdes.String(), responseContainerSerde));
        processedRequest.filter((emptyKey, response) -> response.isStartedAWorkflow() && response.getResponseEntity() != null)
                .foreach((emptyKey, response) -> threadPool.execute(new WorkflowStatusMonitor(resourceHandler, objectMapper,
                        producer, apexResponsesTopic, response.getKey(),
                        (String) response.getResponseEntity(), workflowStatusPollingInterval)));
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
     * Builds and start the kafka stream topology and kafka stream object for processing
     * client requests to Conductor API.
     */
    public void startStream() {
        // Build the topology
        Topology streamsTopology = buildStreamTopology();
        logger.debug("Requests Topology Description: {}", streamsTopology.describe());
        // Build/Create Kafka Streams object for starting and processing via kafka streams
        KafkaStreams clientRequestStream = buildKafkaStream(streamsTopology);
        // Sleep Thread to make sure the server is up before processing requests to Conductor
        sleepThread();
        // Start stream
        logger.info("Starting Kafka Streams for processing client requests to Conductor API");
        clientRequestStream.start();
        // Add shutdown hook to respond to SIGTERM and gracefully close the Streams application.
        Runtime.getRuntime().addShutdownHook(new Thread(clientRequestStream::close));
    }

    /**
     * Execute a Thread sleep for the established time
     */
    private void sleepThread(){
        // Thread.sleep function is executed so that the kafka stream processing of requests are not sent
        // to Conductor before the server is started
        try {
            Thread.sleep(startupTreadSleep);
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
        // This function have not been implemented yet
        logger.error("Called the observe function, not implemented yet.");
        // Restores the interrupt by the InterruptedException so that caller can see that
        // interrupt has occurred.
        Thread.currentThread().interrupt();
        throw new UnsupportedOperationException();
    }

    /**
     * Get type of queue
     * @return Type of queue
     */
    @Override
    public String getType() {
        return QUEUE_TYPE;
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
        return queueName;
    }

    /**
     * Used to acknowledge Kafka Consumer that the message at the current offset was consumed by subscriber
     *
     * @param messages messages to be ack'ed
     * @return Empty List: An empty list is returned due to this method be an implementation of the ObservableQueue interface
     */
    @Override
    public List<String> ack(final List<Message> messages) {
        // This function have not been implemented yet
        logger.error("Called the ack function, not implemented yet.");
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
    public void publish(final List<Message> messages) {
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
    public void setUnackTimeout(final Message message, final long unackTimeout) {
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
     * Creates a separate thread from the main thread for using Kafka Streams to
     * process client requests to Conductor API
     */
    @Override
    public void run() {
        try {
            startStream();
        } catch (final Exception e) {
            logger.error("KafkaStreamsObservableQueue.startStream(), exiting due to error! {}", e.getMessage());
        }
    }
}
