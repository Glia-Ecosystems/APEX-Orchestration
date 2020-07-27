package com.netflix.conductor.contribs.kafka;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.contribs.kafka.resource.RequestContainer;
import com.netflix.conductor.contribs.kafka.resource.handlers.ResourceHandler;
import com.netflix.conductor.contribs.kafka.streams.RequestContainerSerde;
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
 * @author Glia Ecosystems
 */
public class KafkaStreamsObservableQueue implements ObservableQueue, Runnable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsObservableQueue.class);
    private static final String QUEUE_TYPE = "kafkaStreams";
    private static final String KAFKA_STREAMS_PREFIX = "kafka.streams.";
    private static final String CLIENT_STORE = "client-store";
    private static final String SERVICES_STORE ="service-store";
    private static final String EVENT_STORE = "event-store";
    private final Properties streamProperties;
    private final String queueName;
    private final String apexRequestsTopic;
    private final String apexResponsesTopic;
    private final ResourceHandler resourceHandler;

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
        this.streamProperties = createStreamsConfig(configuration);
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
        final Properties streamsProperties = new Properties();

        // Checks if configuration file is not null
        final Map<String, Object> configurationMap = configuration.getAll();
        if (Objects.isNull(configurationMap)) {
            throw new NullPointerException("Configuration missing");
        }
        // Filter through configuration file to get the necessary properties for Kafka Streams
        configurationMap.forEach((key, value) -> {
            if (key.startsWith(KAFKA_STREAMS_PREFIX)) {
                streamsProperties.put(key.replaceAll(KAFKA_STREAMS_PREFIX, ""), value);
            }
        });
        // apply default configs
        applyConsumerDefaults(streamsProperties);
        // Verifies properties
        checkStreamsProperties(streamsProperties);
        return streamsProperties;
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
     * @param streamsProperties Properties object for providing the necessary properties to Kafka Streams
     */
    private void checkStreamsProperties(final Properties streamsProperties) {
        final List<String> mandatoryKeys = Arrays.asList(StreamsConfig.APPLICATION_ID_CONFIG,
                                                         StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
        final List<String> keysNotFound = hasKeyAndValue(streamsProperties, mandatoryKeys);
        if (!keysNotFound.isEmpty()) {
            logger.error("Configuration missing for Kafka streams. {}", keysNotFound);
            throw new IllegalStateException("Configuration missing for Kafka streams." + keysNotFound.toString());
        }
    }

    /**
     * Validates whether the property has given keys.
     *
     * @param streamProperties Properties object for providing the necessary properties to Kafka Streams
     * @param keys             List of the names of mandatory kafka properties needed:
     *      *                                         [APPLICATION_ID_CONFIG, BOOTSTRAP_SERVERS_CONFIG,]
     * @return List of mandatory properties missing from the configuration file
     */
    private List<String> hasKeyAndValue(final Properties streamProperties, final List<String> keys) {
        return keys.stream()
                .filter(key -> !streamProperties.containsKey(key) || Objects.isNull(streamProperties.get(key)))
                .collect(Collectors.toList());
    }

    /**
     * Creates the topology for processing the client initial request to the Conductor API
     *
     * @return A kafka streams topology for processing client requests
     */
    private Topology buildRequestStreamTopology(){
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, RequestContainer> requestStream = builder.stream(apexRequestsTopic,
                Consumed.with(Serdes.String(), new RequestContainerSerde(new JsonMapperProvider().get())));
        return builder.build();
    }

    public void startStream(){
        Topology requestTopology = buildRequestStreamTopology();
        logger.debug("Requests Topology Description: {}", requestTopology.describe());
        KafkaStreams streams = new KafkaStreams(requestTopology, streamProperties);
        // here you should examine the throwable/exception and perform an appropriate action!
        streams.setUncaughtExceptionHandler((Thread thread, Throwable throwable) -> {
            System.out.println(throwable);
        });
        try {
            streams.start();
        } finally {
            streams.close();
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
        }
    }
}
