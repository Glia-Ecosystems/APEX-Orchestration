package com.netflix.conductor.contribs.kafka;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.contribs.kafka.model.RequestContainer;
import com.netflix.conductor.contribs.kafka.model.ResponseContainer;
import com.netflix.conductor.contribs.kafka.resource.handlers.ResourceHandler;
import com.netflix.conductor.contribs.kafka.streamsutil.RequestContainerSerde;
import com.netflix.conductor.contribs.kafka.streamsutil.ResponseContainerSerde;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
    private static final String KAFKA_STREAMS_PREFIX = "kafka.streams.";
    private static final String CLIENT_STORE = "client-store";
    private static final String SERVICES_STORE ="service-store";
    private static final String EVENT_STORE = "event-store";
    private static final int workflowMonitorBranchIndex = 0;
    private final Properties streamsProperties;
    private final String queueName;
    private final String apexRequestsTopic;
    private final String apexResponsesTopic;
    private final ResourceHandler resourceHandler;
    private KafkaStreams builtStreams;

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
        this.queueName = "";
        this.apexRequestsTopic = requestTopic;
        this.apexResponsesTopic = responseTopic;
        this.resourceHandler = new ResourceHandler(injector, new JsonMapperProvider().get());
        this.streamsProperties = createStreamsConfig(configuration);
    }

    /**
     * Builds the properties for kafka streams with the properties of prefix 'kafka.streams.'
     * from the provided configuration. Queue name (Topic) is provided from the workflow if kafka is
     * initialized in a event queue or provided from the configuration if kafka streams is initialize for
     * processing client requests to Conductor API. It is/should be assumed that the topics provided are already
     * configured in the kafka cluster. Fails if any mandatory configs are missing.
     *
     * @param configuration Main configuration file for the Conductor application
     * @return Properties file for kafka streams configuration
     */
    private Properties createStreamsConfig(final Configuration configuration) {
        // You must set the properties in the .properties files
        final Properties properties = new Properties();

        // Checks if configuration file is not null
        final Map<String, Object> configurationMap = configuration.getAll();
        if (Objects.isNull(configurationMap)) {
            throw new NullPointerException("Configuration missing");
        }
        // Filter through configuration file to get the necessary properties for Kafka Streams
        configurationMap.forEach((key, value) -> {
            if (key.startsWith(KAFKA_STREAMS_PREFIX)) {
                properties.put(key.replaceAll(KAFKA_STREAMS_PREFIX, ""), value);
            }
        });
        // apply default configs
        applyConsumerDefaults(properties);
        // Verifies properties
        checkStreamsProperties(properties);
        return properties;
    }

    /**
     * Apply Kafka consumer default properties, if not configured in configuration given file.
     *
     * @param streamsProperties  Properties object for providing the necessary properties to Kafka Streams
     */
    private void applyConsumerDefaults(final Properties streamsProperties) {
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

    private Topology buildRecoveryStreamTopology(){
        // This function have not been implemented yet
        logger.error("Called a function not implemented yet.");
        // Restores the interrupt by the InterruptedException so that caller can see that
        // interrupt has occurred.
        Thread.currentThread().interrupt();
        throw new UnsupportedOperationException();
    }

    /**
     * Creates the topology for processing the client initial request to the Conductor API
     *
     * @return A kafka streams topology for processing client requests
     */
    private Topology buildRequestsProcessorStreamTopology(){
        RequestContainerSerde requestContainerSerde = new RequestContainerSerde(new JsonMapperProvider().get());
        ResponseContainerSerde responseContainerSerde = new ResponseContainerSerde();
        StreamsBuilder builder = new StreamsBuilder();
        // Parent Node
        // Source Node (Responsible for consuming the records from a given topic, that will be processed)
        KStream<String, RequestContainer> requestStream = builder.stream(apexRequestsTopic,
                Consumed.with(Serdes.String(), requestContainerSerde))
                .peek((k, v) -> logger.info("Received record. Client: {} Request: {}", k, v));
        // Child Node of Parent Node - Execute Request to Conductor API and receive response
        KStream<String, ResponseContainer> executeStream = requestStream.mapValues(resourceHandler::processRequest);
        // Sink Node - Send Response to client
        executeStream.to(apexResponsesTopic, Produced.with(Serdes.String(), responseContainerSerde));
        return builder.build();
    }

    public void startStream() {
        Topology requestTopology = buildRequestsProcessorStreamTopology();
        logger.debug("Requests Topology Description: {}", requestTopology.describe());
        builtStreams = new KafkaStreams(requestTopology, streamsProperties);
        // here you should examine the throwable/exception and perform an appropriate action!
        builtStreams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) ->
            logger.error(String.valueOf(throwable)));
        sleepThread();
        logger.info("Starting Kafka Streams for processing client requests to Conductor API");
        builtStreams.start();
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
        } finally {
            try {
                // Try to disconnect all connections to kafka via a consumer or producer object
                close();
            } catch (final Exception e) {
                logger.error("KafkaStreamsObservableQueue.close(), unable to complete kafka clean up! {}", e.getMessage());
            }
        }
    }
}
