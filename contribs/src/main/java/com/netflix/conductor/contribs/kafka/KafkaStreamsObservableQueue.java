package com.netflix.conductor.contribs.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.contribs.kafka.model.RequestContainer;
import com.netflix.conductor.contribs.kafka.model.ResponseContainer;
import com.netflix.conductor.contribs.kafka.model.WorkflowStatusMonitor;
import com.netflix.conductor.contribs.kafka.resource.handlers.ResourceHandler;
import com.netflix.conductor.contribs.kafka.streamsutil.KafkaStreamsDeserializationExceptionHandler;
import com.netflix.conductor.contribs.kafka.streamsutil.KafkaStreamsProductionExceptionHandler;
import com.netflix.conductor.contribs.kafka.streamsutil.RequestContainerSerde;
import com.netflix.conductor.contribs.kafka.streamsutil.ResponseContainerSerde;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.core.execution.ApplicationException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
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
    private static final String KAFKA_PREFIX = "kafka.";
    private static final String KAFKA_STREAMS_PREFIX = "kafka.streams.";
    private static final String KAFKA_PRODUCER_PREFIX = "kafka.producer.";
    private static final int EXECUTE_BRANCH = 0;
    private static final int ERROR_BRANCH = 1;
    // Create custom Serde objects for processing records
    private static final RequestContainerSerde requestContainerSerde = new RequestContainerSerde(new JsonMapperProvider().get());
    private static final ResponseContainerSerde responseContainerSerde = new ResponseContainerSerde();
    private final ObjectMapper objectMapper = new JsonMapperProvider().get();
    private final Properties streamsProperties;
    private final String queueName;
    private final String apexRequestsTopic;
    private final String apexResponsesTopic;
    private final ResourceHandler resourceHandler;
    private KafkaStreams builtStreams;
    private KafkaProducer<String, String> producer;
    private final ExecutorService threadPool;

    /**
     * Constructor of the KafkaStreamsObservableQueue for using kafka streams processing
     * client requests to Conductor API and publishing responses from Conductor API to client
     *
     * @param injector      Google Dependency Injector object that builds the graph of objects for applications
     * @param configuration Main configuration file for the Conductor application
     * @param requestTopic  Topic for consuming messages
     * @param responseTopic Topic for publishing messages
     */
    @Inject
    public KafkaStreamsObservableQueue(final Injector injector, final Configuration configuration,
                                       final String requestTopic, final String responseTopic){
        final Map<String, Object> configurationMap = getConfigurationMap(configuration);
        this.queueName = "";
        this.apexRequestsTopic = requestTopic;
        this.apexResponsesTopic = responseTopic;
        this.resourceHandler = new ResourceHandler(injector, new JsonMapperProvider().get());
        this.threadPool = Executors.newFixedThreadPool(configuration.getIntProperty("conductor.kafka.listener.thread.pool", 20));
        this.streamsProperties = createStreamsConfig(configurationMap);
        initKafkaProducer(configurationMap);
    }

    /**
     * Builds the properties for kafka streams with the properties of prefix 'kafka.streams.'
     * from the provided configuration. Queue name (Topic) is provided from the workflow if kafka is
     * initialized in a event queue or provided from the configuration if kafka streams is initialize for
     * processing client requests to Conductor API. It is/should be assumed that the topics provided are already
     * configured in the kafka cluster. Fails if any mandatory configs are missing.
     *
     * @param configurationMap Map object of the configuration file for the Conductor application
     * @return Properties file for kafka streams configuration
     */
    private Properties createStreamsConfig(final Map<String, Object> configurationMap) {
        // You must set the properties in the .properties files
        final Properties properties = new Properties();

        // Filter through configuration file to get the necessary properties for Kafka Streams
        configurationMap.forEach((key, value) -> {
            if (key.startsWith(KAFKA_STREAMS_PREFIX)) {
                properties.put(key.replaceAll(KAFKA_STREAMS_PREFIX, ""), value);
            }
        });
        // apply default configs
        applyKafkaStreamsConsumerDefaults(properties);
        // apply exception handlers configs
        setKafkaStreamsDeserializationExceptionHandler(properties);
        setKafkaStreamsProductionExceptionHandler(properties);
        // Verifies properties
        checkStreamsProperties(properties);
        return properties;
    }

    /**
     * Initializes the kafka  producer with the properties of prefix 'kafka.producer.' and 'kafka.' from the
     * provided configuration. Fails if any mandatory configs are missing.
     *
     * @param configurationMap Map object of the configuration file for the Conductor application
     */
    private void initKafkaProducer(final Map<String, Object> configurationMap) {
        final Properties producerProperties = new Properties();

        // Filter through configuration file to get the necessary properties for Kafka Streams
        configurationMap.forEach((key, value) -> {
            if (key.startsWith(KAFKA_PREFIX)) {
                if (key.startsWith(KAFKA_PRODUCER_PREFIX)) {
                    producerProperties.put(key.replaceAll(KAFKA_PRODUCER_PREFIX, ""), value);
                } else {
                    producerProperties.put(key.replaceAll(KAFKA_PREFIX, ""), value);
                }
            }
        });
        // Verifies properties
        checkProducerProperties(producerProperties);
        // Init Kafka producer
        producer = new KafkaProducer<>(producerProperties);
    }

    /**
     * Apply Kafka consumer default properties, if not configured in configuration given file.
     *
     * @param streamsProperties  Properties object for providing the necessary properties to Kafka Streams
     */
    private void applyKafkaStreamsConsumerDefaults(final Properties streamsProperties) {
        if (null == streamsProperties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
            streamsProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        }
    }

    /**
     * Checks that the mandatory configurations are available for kafka streams
     *
     * @param properties Properties object for providing the necessary properties to Kafka Streams
     */
    private void checkStreamsProperties(final Properties properties) {
        final List<String> mandatoryKeys = Arrays.asList(StreamsConfig.APPLICATION_ID_CONFIG,
                                                         StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
        final List<String> keysNotFound = hasKeyAndValue(properties, mandatoryKeys);
        if (!keysNotFound.isEmpty()) {
            logger.error("Configuration missing for Kafka streams. {}", keysNotFound);
            throw new IllegalStateException("Configuration missing for Kafka streams." + keysNotFound.toString());
        }
    }

    /**
     * Checks that the mandatory configurations are available for kafka producer.
     *
     * @param producerProperties Kafka Properties object for providing the necessary properties to Kafka Producer
     */
    private void checkProducerProperties(final Properties producerProperties) {
        final List<String> mandatoryKeys = Arrays.asList(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
        final List<String> keysNotFound = hasKeyAndValue(producerProperties, mandatoryKeys);
        if (!keysNotFound.isEmpty()) {
            logger.error("Configuration missing for Kafka producer. {}", keysNotFound);
            throw new IllegalStateException("Configuration missing for Kafka producer." + keysNotFound.toString());
        }
    }

    /**
     * Set a custom deserialization exception handler in kafka streams config
     *
     * @param properties Properties object for providing the necessary properties to Kafka Streams
     */
    private void setKafkaStreamsDeserializationExceptionHandler(final Properties properties){
        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                KafkaStreamsDeserializationExceptionHandler.class);
    }

    /**
     * Set a custom production/producer exception handler in kafka streams config
     *
     * @param properties Properties object for providing the necessary properties to Kafka Streams
     */
    private void setKafkaStreamsProductionExceptionHandler(final Properties properties){
        properties.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
                KafkaStreamsProductionExceptionHandler.class);
    }

    /**
     * Validates whether the property has given keys.
     *
     * @param properties Properties object for providing the necessary properties to Kafka Streams
     * @param keys             List of the names of mandatory kafka properties needed:
     *      *                                         [APPLICATION_ID_CONFIG, BOOTSTRAP_SERVERS_CONFIG,]
     * @return List of mandatory properties missing from the configuration file
     */
    private List<String> hasKeyAndValue(final Properties properties, final List<String> keys) {
        return keys.stream()
                .filter(key -> !properties.containsKey(key) || Objects.isNull(properties.get(key)))
                .collect(Collectors.toList());
    }

    /**
     * Create a configuration map of the given config file and make verify that thee config file
     * is not null
     *
     * @param configuration Main configuration file for the Conductor application
     * @return Map object of the configuration file
     */
    private Map<String, Object> getConfigurationMap(Configuration configuration){
        final Map<String, Object> configurationMap = configuration.getAll();
        // Checks if configuration file is not null
        if (Objects.isNull(configurationMap)) {
            throw new NullPointerException("Configuration missing");
        }
        return configurationMap;
    }

    /**
     * Creates the topology for processing the client initial request to the Conductor API
     *
     * @return A kafka streams topology for processing client requests
     */
    private Topology buildStreamTopology(){
        logger.info("Building Kafka Streams Topology for handling requests and responses to/from Conductor");
        // Build kafka streams topology
        StreamsBuilder builder = new StreamsBuilder();
        // Parent Node
        // Source Node (Responsible for consuming the records from a given topic, that will be processed)
        KStream<String, RequestContainer> requestStream = builder.stream(apexRequestsTopic,
                Consumed.with(Serdes.String(), requestContainerSerde))
                .peek((k, v) -> logger.info("Received record. Client: {} Request: {}", k, v));
        // Branch Processor Node
        // Each record is matched against the given predicates in the order that they're provided.
        // The branch processor will assign records to a stream on the first match.
        // WARNING: No attempts are made to match additional predicates.
        // If no errors occurred during deserialization, process request further
        Predicate<String, RequestContainer> readyToProcess = (clientId, request) -> !request.isDeserializationErrorOccurred();
        // If an error occurred, send error to client who made initial request
        Predicate<String, RequestContainer> isError = (clientId, request) -> request.isDeserializationErrorOccurred();
        KStream<String, RequestContainer>[] executeDept = requestStream.branch(readyToProcess, isError);
        // Child Node - Execute Request to Conductor API and receive response
        KStream<String, ResponseContainer> processedRequest = executeDept[EXECUTE_BRANCH].mapValues(resourceHandler::processRequest);
        // Child Node - Process error
        KStream<String, ResponseContainer> processedError = executeDept[ERROR_BRANCH].mapValues(KafkaStreamsDeserializationExceptionHandler::processError);
        // Sink Node - Send Response to client
        processedRequest.to(apexResponsesTopic, Produced.with(Serdes.String(), responseContainerSerde));
        // Sink Node - Send Error to client
        processedError.to(apexResponsesTopic, Produced.with(Serdes.String(), responseContainerSerde));
        processedRequest.filter((clientId, response) -> response.isStartedAWorkflow())
                 .foreach((client, response) -> threadPool.execute(new WorkflowStatusMonitor(resourceHandler, objectMapper,
                         this, client, (String) response.getResponseEntity())));
        return builder.build();
    }

    /**
     * Create a KafkaStreams object containing the built topology and properties file
     * for processing requests to the Conductor API via kafka streams.
     *
     * @param streamsTopology A topology object containing the structure of the kafka streams
     */
    private void buildKafkaStreams(Topology streamsTopology) {
        builtStreams = new KafkaStreams(streamsTopology, streamsProperties);
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
        builtStreams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) ->
                // You can make it restart the stream, but you have to make sure that this thread is
                // destroyed once a new thread is spawned to restart the kafka streams
                logger.error(String.valueOf(throwable)));
    }

    /**
     * Builds and start the kafka stream topology and kafka stream object for processing
     * client requests to Conductor API.
     */
    public void startStream() {
        // Build the topology
        Topology streamsTopology = buildStreamTopology();
        logger.info("Requests Topology Description: {}", streamsTopology.describe());
        // Build/Create Kafka Streams object for starting and processing via kafka streams
        buildKafkaStreams(streamsTopology);
        // Sleep Thread to make sure the server is up before processing requests to Conductor
        sleepThread();
        // Start stream
        logger.info("Starting Kafka Streams for processing client requests to Conductor API");
        builtStreams.start();
        // Add shutdown hook to respond to SIGTERM and gracefully close the Streams application.
        Runtime.getRuntime().addShutdownHook(new Thread(builtStreams::close));
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
     * Closing of connections to Kafka Streams
     */
    @Override
    public void close() {
        if (builtStreams != null) {
            builtStreams.close();
        }
    }

    /**
     * Publish the messages to the given topic.
     *
     * @param key Key of the record to be publish
     * @param value Value of the record to be publish
     */
    public void publishMessage(final String key, final String value) {
        final ProducerRecord<String, String> record = new ProducerRecord<>(apexResponsesTopic, key,
                value);
        final RecordMetadata metadata;
        try {
            metadata = producer.send(record).get();
            final String producerLogging = "Producer Record: key " + record.key() + ", value " + record.value() +
                    ", partition " + metadata.partition() + ", offset " + metadata.offset();
            logger.debug(producerLogging);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Publish message to kafka topic {} failed with an error: {}", apexResponsesTopic, e.getMessage(), e);
        } catch (final ExecutionException e) {
            logger.error("Publish message to kafka topic {} failed with an error: {}", apexResponsesTopic, e.getMessage(), e);
            throw new ApplicationException(ApplicationException.Code.INTERNAL_ERROR, "Failed to publish the event");
        }
        logger.info("Message published to kafka topic {}. key/client {}", apexResponsesTopic, key);
    }

    /**
     * Provide RX Observable object for consuming messages from Kafka Consumer
     * @return Observable object
     */
    @Override
    public Observable<Message> observe() {
        // This function have not been implemented yet
        logger.error("Called a function not implemented yet.");
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
