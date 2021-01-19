package com.netflix.conductor.contribs.kafka.config;

import com.netflix.conductor.contribs.kafka.streamsutil.KafkaStreamsDeserializationExceptionHandler;
import com.netflix.conductor.contribs.kafka.streamsutil.KafkaStreamsProductionExceptionHandler;
import com.netflix.conductor.contribs.kafka.streamsutil.TestExceptionHandler;
import com.netflix.conductor.core.config.Configuration;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class KafkaPropertiesProvider {

    private static final Logger logger = LoggerFactory.getLogger(KafkaPropertiesProvider.class);
    private static final String KAFKA_PREFIX = "kafka.";
    private static final String KAFKA_ADMIN_PREFIX = "kafka.admin.";
    private static final String KAFKA_PRODUCER_PREFIX = "kafka.producer.";
    private static final String KAFKA_CONSUMER_PREFIX = "kafka.consumer.";
    private static final String KAFKA_STREAMS_PREFIX = "kafka.streams.";
    private final Map<String, Object> configurationMap;
    private final Configuration configuration;

    public KafkaPropertiesProvider(final Configuration configuration){
        this.configuration = configuration;
        this.configurationMap = getConfigurationMap(configuration);
    }

    /**
     * Builds the properties for kafka streams with the properties of prefix 'kafka.streams.'
     * from the provided configuration. Queue name (Topic) is provided from the workflow if kafka is
     * initialized in a event queue or provided from the configuration if kafka streams is initialize for
     * processing client requests to Conductor API. It is/should be assumed that the topics provided are already
     * configured in the kafka cluster. Fails if any mandatory configs are missing.
     *
     * @return Properties file for kafka streams configuration
     */
    public Properties getStreamsProperties(final String streamID){
        final Properties streamsProperties = new Properties();
        // Filter through configuration file to get the necessary properties for Kafka Streams
        configurationMap.forEach((key, value) -> {
            if (key.startsWith(KAFKA_STREAMS_PREFIX)) {
                streamsProperties.put(key.replaceAll(KAFKA_STREAMS_PREFIX, ""), value);
            }
        });
        // apply default configs
        applyKafkaStreamsConsumerDefaults(streamsProperties);
        // apply exception handlers configs
        setKafkaStreamsDeserializationExceptionHandler(streamsProperties);
        setKafkaStreamsProductionExceptionHandler(streamsProperties);
        // Verifies properties
        checkStreamsProperties(streamsProperties);
        // Set unique application ID
        String applicationID = (String) streamsProperties.get(StreamsConfig.APPLICATION_ID_CONFIG);
        streamsProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationID + "-" + streamID);
        return streamsProperties;
    }

    /**
     * Creates the properties file for the kafka producer of prefix 'kafka.producer.' and 'kafka.' from the
     * provided configuration. Fails if any mandatory configs are missing.
     *
     * @return Properties file for kafka producer configuration
     */
    public Properties getProducerProperties(){
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
        return producerProperties;
    }

    /**
     * Creates the properties file for the kafka consumer of prefix 'kafka.consumer.' and 'kafka.' from the
     * provided configuration. Fails if any mandatory configs are missing.
     *
     * @return Properties file for kafka producer configuration
     */
    public Properties getConsumerProperties(){
        final Properties consumerProperties = new Properties();
        // Filter through configuration file to get the necessary properties for Kafka Streams
        configurationMap.forEach((key, value) -> {
            if (key.startsWith(KAFKA_PREFIX)) {
                if (key.startsWith(KAFKA_CONSUMER_PREFIX)) {
                    consumerProperties.put(key.replaceAll(KAFKA_CONSUMER_PREFIX, ""), value);
                } else {
                    consumerProperties.put(key.replaceAll(KAFKA_PREFIX, ""), value);
                }
            }
        });
        // Verifies properties
        checkConsumerProperties(consumerProperties);
        // Apply default properties for Kafka Consumer if not configured in configuration file
        applyConsumerDefaults(consumerProperties);
        return consumerProperties;
    }

    /**
     * Creates the properties file for the kafka admin of prefix 'kafka.admin.' from the
     * provided configuration. Fails if any mandatory configs are missing.
     *
     * @return Properties file for kafka admin configuration
     */
    public Properties getAdminProperties(){
        final Properties adminProperties = new Properties();
        // Filter through configuration file to get the necessary properties for Kafka Streams
        configurationMap.forEach((key, value) -> {
            if (key.startsWith(KAFKA_PREFIX)) {
                if (key.startsWith(KAFKA_ADMIN_PREFIX)) {
                    adminProperties.put(key.replaceAll(KAFKA_ADMIN_PREFIX, ""), value);
                } else {
                    adminProperties.put(key.replaceAll(KAFKA_PREFIX, ""), value);
                }
            }
        });
        // Verifies properties
        checkAdminProperties(adminProperties);
        return adminProperties;
    }

    /**
     * Create a configuration map of the given config file and make verify that the config file
     * is not null
     *
     * @param configuration Main configuration file for the Conductor application
     * @return Map object of the configuration file
     */
    private Map<String, Object> getConfigurationMap(final Configuration configuration){
        final Map<String, Object> configMap = configuration.getAll();
        // Checks if configuration file is not null
        if (Objects.isNull(configMap)) {
            throw new NullPointerException("Configuration missing");
        }
        return configMap;
    }

    /**
     * Checks that the mandatory configurations are available for kafka admin
     *
     * @param properties
     */
    private void checkAdminProperties(final Properties properties){
        final List<String> mandatoryKeys = Arrays.asList(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                AdminClientConfig.CLIENT_ID_CONFIG);
        final List<String> keysNotFound = hasKeyAndValue(properties, mandatoryKeys);
        if (!keysNotFound.isEmpty()) {
            logger.error("Configuration missing for Kafka admins. {}", keysNotFound);
            throw new IllegalStateException("Configuration missing for Kafka admins." + keysNotFound.toString());
        }
    }

    /**
     * Checks that the mandatory configurations are available for kafka streams
     *
     * @param properties Properties object for providing the necessary properties to Kafka Admin
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
     * Checks that the mandatory configurations are available for the kafka consumer.
     *
     * @param consumerProperties `Kafka Properties object for providing the necessary properties to Kafka Consumer`
     */
    private void checkConsumerProperties(final Properties consumerProperties) {
        final List<String> mandatoryKeys = Arrays.asList(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        final List<String> keysNotFound = hasKeyAndValue(consumerProperties, mandatoryKeys);
        if (!keysNotFound.isEmpty()) {
            logger.error("Configuration missing for Kafka consumer. {}", keysNotFound);
            throw new IllegalStateException("Configuration missing for Kafka consumer." + keysNotFound.toString());
        }
    }

    /**
     * Set a custom deserialization exception handler in kafka streams config
     *
     * @param properties Properties object for providing the necessary properties to Kafka Streams
     */
    private void setKafkaStreamsDeserializationExceptionHandler(Properties properties){
        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                KafkaStreamsDeserializationExceptionHandler.class);
    }

    /**
     * Set a custom production/producer exception handler in kafka streams config
     *
     * @param properties Properties object for providing the necessary properties to Kafka Streams
     */
    private void setKafkaStreamsProductionExceptionHandler(Properties properties){
        properties.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
                new KafkaStreamsProductionExceptionHandler(configuration, getProducerProperties()).getProductionExceptionHandler());
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
     * Apply Kafka consumer default properties, if not configured in configuration given file.
     *
     * @param consumerProperties Kafka Properties object for providing the necessary properties to Kafka Consumer
     */
    private void applyConsumerDefaults(final Properties consumerProperties) {
        if (null == consumerProperties.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)) {
            consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        }
        if (null == consumerProperties.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)) {
            consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        }
    }
}
