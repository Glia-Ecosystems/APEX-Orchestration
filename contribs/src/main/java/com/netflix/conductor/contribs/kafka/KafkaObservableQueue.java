package com.netflix.conductor.contribs.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.contribs.kafka.resource.handlers.ResourceHandler;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.core.execution.ApplicationException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Observable.OnSubscribe;

import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Reads the properties with prefix 'kafka.producer.', 'kafka.consumer.' and 'kafka.' from the
 * provided configuration. Initializes a producer and consumer based on the given value. Queue name
 * is driven from the workflow. It is assumed that the queue name provided is already configured in
 * the kafka cluster.
 *
 * @author Glia Ecosystems
 */
public class KafkaObservableQueue implements ObservableQueue, Runnable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaObservableQueue.class);
    private static final String QUEUE_TYPE = "kafka";
    private static final String KAFKA_PREFIX = "kafka.";
    private static final String KAFKA_PRODUCER_PREFIX = "kafka.producer.";
    private static final String KAFKA_CONSUMER_PREFIX = "kafka.consumer.";
    private final String queueName;
    private final String listenerConsumerTopic;
    private final String listenerProducerTopic;
    private final boolean listenerEnabled;
    private final int pollIntervalInMS;
    private final Duration pollTimeoutInMs;
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;
    private final AtomicReference<Thread> readThread = new AtomicReference<>(); // May delete
    private final ExecutorService threadPool;
    private KafkaMessageHandler kafkaMessageHandler;


    /**
     * Constructor of KafkaObservableQueue for Conductor Event/System tasks
     *
     * @param queueName Topic for consuming or publishing messages via kafka
     * @param config    Main configuration file for the Conductor application
     */
    @Inject
    public KafkaObservableQueue(final String queueName, final Configuration config) {
        this.queueName = queueName;  // Topic
        this.pollIntervalInMS = config.getIntProperty("kafka.consumer.pollingInterval", 1000);
        this.pollTimeoutInMs = Duration.ofMillis(config.getIntProperty("kafka.consumer.longPollTimeout", 1000));
        this.listenerProducerTopic = "";
        this.listenerConsumerTopic = "";
        this.listenerEnabled = false;
        this.threadPool = null;
        init(config);  // Init Kafka producer and consumer properties
    }

    /**
     * Constructor of KafkaObservableQueue for a kafka listener object for consuming and processing client
     * requests to Conductor API and publishing responses from Conductor API to client
     *
     * @param consumerTopic Topic for consuming messages
     * @param producerTopic Topic for publishing messages
     * @param config        Main configuration file for the Conductor application
     * @param injector      Google Dependency Injector object that builds the graph of objects for applications
     */
    @Inject
    public KafkaObservableQueue(final String consumerTopic, final String producerTopic,
                                final Configuration config, final Injector injector) {
        this.queueName = "";
        this.kafkaMessageHandler = new KafkaMessageHandler(new ResourceHandler(injector, new JsonMapperProvider().get()),
                new JsonMapperProvider().get());
        this.pollIntervalInMS = config.getIntProperty("kafka.consumer.pollingInterval", 1000);
        this.pollTimeoutInMs = Duration.ofMillis(config.getIntProperty("kafka.consumer.longPollTimeout", 1000));
        this.listenerConsumerTopic = consumerTopic;
        this.listenerProducerTopic = producerTopic;
        this.listenerEnabled = true;
        this.threadPool = Executors.newFixedThreadPool(config.getIntProperty("conductor.kafka.listener.thread.pool", 20));
        init(config);  // Init Kafka producer and consumer properties
    }

    /**
     * Initializes the kafka consumer and producer with the properties of prefix 'kafka.producer.', 'kafka.consumer.'
     * and 'kafka.' from the provided configuration. Queue name (Topic) is provided from the workflow if kafka is
     * initialized in a event queue or provided from the configuraation if kafka is initialize for processing client
     * requests to Conductor API. It is/should be assumed that the queue name provided is already configured in
     * the kafka cluster. Fails if any mandatory configs are missing.
     *
     * @param config Main configuration file for the Conductor application
     */
    private void init(final Configuration config) {
        // You must set the properties in the .properties files first for creating a producer/consumer object
        final Properties producerProperties = new Properties();
        final Properties consumerProperties = new Properties();
        final String serverId = config.getServerId();
        if (listenerEnabled) {
            consumerProperties.put("group.id", listenerConsumerTopic + "_group");
            consumerProperties.put("client.id", listenerConsumerTopic + "_consumer_" + serverId);
            producerProperties.put("client.id", listenerProducerTopic + "_producer_" + serverId);
        } else {
            consumerProperties.put("group.id", queueName + "_group");
            consumerProperties.put("client.id", queueName + "_consumer_" + serverId);
            producerProperties.put("client.id", queueName + "_producer_" + serverId);
        }
        final Map<String, Object> configurationMap = config.getAll();
        if (Objects.isNull(configurationMap)) {
            throw new NullPointerException("Configuration missing");
        }
        // Filter through configuration file to get the necessary properties for Kafka producer and consumer
        for (final Entry<String, Object> entry : configurationMap.entrySet()) {
            final String key = entry.getKey();
            final String value = (String) entry.getValue();
            if (key.startsWith(KAFKA_PREFIX)) {
                if (key.startsWith(KAFKA_PRODUCER_PREFIX)) {
                    producerProperties.put(key.replaceAll(KAFKA_PRODUCER_PREFIX, ""), value);
                } else if (key.startsWith(KAFKA_CONSUMER_PREFIX)) {
                    consumerProperties.put(key.replaceAll(KAFKA_CONSUMER_PREFIX, ""), value);
                } else {
                    producerProperties.put(key.replaceAll(KAFKA_PREFIX, ""), value);
                    consumerProperties.put(key.replaceAll(KAFKA_PREFIX, ""), value);
                }
            }
        }
        // Verifies properties
        checkProducerProperties(producerProperties);
        checkConsumerProperties(consumerProperties);
        // Apply default properties for Kafka Consumer if not configured in configuration file
        applyConsumerDefaults(consumerProperties);

        try {
            // Init Kafka producer and consumer
            producer = new KafkaProducer<>(producerProperties);
            consumer = new KafkaConsumer<>(consumerProperties);
            // Assumption is that the consumer topic (queueName/listenerConsumerTopic) provided is already configured
            // within the Kafka cluster.
            // This is where Consumer subscribe to given Topic
            if (listenerEnabled) {
                consumer.subscribe(Collections.singletonList(listenerConsumerTopic));
                logger.info("KafkaObservableQueue initialized. Listening to {} (consumer topic) for consuming and " +
                                "processing client requests. Responses will be published to {} (producer topic)",
                        listenerConsumerTopic, listenerProducerTopic);
            } else {
                consumer.subscribe(Collections.singletonList(queueName));
                logger.info("KafkaObservableQueue initialized for {} topic", queueName);
            }
        } catch (final KafkaException ke) {
            throw new KafkaException("Kafka initialization failed.", ke.getCause());
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

    /**
     * Validates whether the property has given keys.
     *
     * @param properties Kafka Properties object for providing the necessary properties to Kafka Consumer/Producer
     * @param keys       List of the names of mandatory kafka properties needed:
     *                   [Bootstrap servers, key serializer, value serializer, key deserializer, value deserializer]
     * @return List of mandatory properties missing from the configuration file
     */
    private List<String> hasKeyAndValue(final Properties properties, final List<String> keys) {
        final List<String> keysNotFound = new ArrayList<>();
        for (final String key : keys) {
            if (!properties.containsKey(key) || Objects.isNull(properties.get(key))) {
                keysNotFound.add(key);
            }
        }
        return keysNotFound;
    }

    /**
     * Provides an RX Observable object for consuming messages from Kafka Consumer
     * @return Observable object
     */
    @VisibleForTesting
    public OnSubscribe<Message> getOnSubscribe() {
        return subscriber -> {
            final Observable<Long> interval = Observable.interval(pollIntervalInMS, TimeUnit.MILLISECONDS);
            interval.flatMap((Long x) -> {
                List<Message> messages = receiveMessages();
                return Observable.from(messages);
            }).subscribe(subscriber::onNext, subscriber::onError);
        };
    }

    /**
     * Polls the provided topic and retrieve the messages.
     *
     * @return List of messages from consumed from Kafka topic
     */
    @VisibleForTesting()
    public List<Message> receiveMessages() {
        final List<Message> messages = new ArrayList<>();
        try {
            final ConsumerRecords<String, String> records = consumer.poll(pollTimeoutInMs);
            if (records.count() == 0) {
                // Currently no messages in the kafka topic
                return messages;
            }
            logger.info("polled {} messages from kafka topic.", records.count());
            records.forEach(record -> {
                logger.debug("Consumer Record: " + "key: {}, " + "value: {}, " + "partition: {}, " + "offset: {}",
                        record.key(), record.value(), record.partition(), record.offset());
                final String id = record.key() + ":" + record.topic() + ":" + record.partition() + ":" + record.offset();
                final Message message = new Message(id, String.valueOf(record.value()), "");
                messages.add(message);
            });
        } catch (final KafkaException e) {
            logger.error("kafka consumer message polling failed. {}", e.getMessage());
        }
        return messages;
    }

    /**
     * Publish the messages to the given topic.
     *
     * @param messages List of messages to be publish via Kafka Producer
     */
    @VisibleForTesting()
    public void publishMessages(final List<Message> messages) {
        String topicForPublishing = (listenerEnabled) ? listenerProducerTopic : queueName;
        if (messages == null || messages.isEmpty()) {
            return;
        }
        for (final Message message : messages) {
            final ProducerRecord<String, String> record = new ProducerRecord<>(topicForPublishing, message.getId(),
                    message.getPayload());
            final RecordMetadata metadata;
            try {
                metadata = producer.send(record).get();
                final String producerLogging = "Producer Record: key " + record.key() + ", value " + record.value() +
                        ", partition " + metadata.partition() + ", offset " + metadata.offset();
                logger.debug(producerLogging);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Publish message to kafka topic {} failed with an error: {}", topicForPublishing, e.getMessage(), e);
            } catch (final ExecutionException e) {
                logger.error("Publish message to kafka topic {} failed with an error: {}", topicForPublishing, e.getMessage(), e);
                throw new ApplicationException(ApplicationException.Code.INTERNAL_ERROR, "Failed to publish the event");
            }
        }
        logger.info("Messages published to kafka topic {}. count {}", topicForPublishing, messages.size());

    }

    /**
     * Provide RX Observable object for consuming messages from Kafka Consumer
     * @return Observable object
     */
    @Override
    public Observable<Message> observe() {
        final OnSubscribe<Message> subscriber = getOnSubscribe();
        return Observable.create(subscriber);
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
        return queueName;
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
        final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        messages.forEach(message -> {
            final String[] idParts = message.getId().split(":");
            currentOffsets.put(new TopicPartition(idParts[1], Integer.parseInt(idParts[2])),
                    new OffsetAndMetadata(Integer.parseInt(idParts[3]) + 1, "no metadata"));
        });
        try {
            consumer.commitSync(currentOffsets);
        } catch (final KafkaException ke) {
            logger.error("kafka consumer selective commit failed.", ke);
            return messages.stream().map(Message::getId).collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    /**
     * Publish message to provided  topic
     *
     * @param messages Messages to be published
     */
    @Override
    public void publish(final List<Message> messages) {
        publishMessages(messages);
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
     * Closing of connections to Kafka Producer/Consumer
     */
    @Override
    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }

        if (consumer != null) {
            consumer.unsubscribe();
            consumer.close();
        }
    }

    /**
     * Executes the process of executing client requests to the Conductor API via
     * Kafka
     */
    public void listen() {
        logger.info("Kafka Listener is now waiting for server to start");
        sleepThread(); // This allows the Kafka thread to run once the server is started
        logger.info("Consuming messages from topic {}. ", listenerConsumerTopic);
        readThread.set(Thread.currentThread());
        // Remove infinite loop
        while (true) {
            // Observable<Message> listener  = observe();
            final List<Message> messages = receiveMessages();
            if (!messages.isEmpty()) {
                messages.forEach(msg -> threadPool.execute(() -> processAndPublishRequestMessage(msg)));
                // Acknowledge messages received and processed upon completion
                ack(messages);
            }
        }
    }

    /**
     * Execute a Thread sleep for the established time
     */
    private void sleepThread(){
        // Thread.sleep function is executed so that a consumed message is not sent
        // to Conductor before the server is started
        try {
            Thread.sleep(45000); // 45 seconds thread sleep
        } catch (final InterruptedException e) {
            // Restores the interrupt by the InterruptedException so that caller can see that
            // interrupt has occurred.
            Thread.currentThread().interrupt();
            logger.error("Error occurred while trying to sleep Thread. {}", e.getMessage());
        }
    }

    /**
     * Send client requests to Conductor API and publish response to client
     *
     * @param message Messages from Kafka topic
     */
    public void processAndPublishRequestMessage(final Message message) {
        final List<Message> responseMessage = new ArrayList<>();
        logger.info("Received and processing message: {}", message.getPayload());
        // A synchronization aid that allows one or more threads to wait upon the
        // completion of another thread via a countdown indicator
        // Used to allow the return of the workflow id, if a startWorkflow was
        // requested before the results of the workflow status.
        CountDownLatch countDownLatch = new CountDownLatch(1);
        responseMessage.add(kafkaMessageHandler.processMessage(message, this, threadPool, countDownLatch));
        publish(responseMessage);
        countDownLatch.countDown();
    }

    /**
     * Creates a separate thread from the main thread for Kafka Listener
     */
    @Override
    public void run() {
        try {
            listen();
        } catch (final Exception e) {
            logger.error("KafkaObservableQueue.listen(), exiting due to error! {}", e.getMessage());
        }finally {
            try {
                // Try to shut down/ terminate all reserved threads in the thread pool
                threadPool.shutdown();
            } catch (final Exception e) {
                logger.error("ThreadPool.shutdown(), unable to complete shutting down threadpool! {}", e.getMessage());
            }
            try {
                // Try to disconnect all connections to kafka via a consumer or producer object
                close();
            } catch (final Exception e) {
                logger.error("KafkaObservableQueue.close(), unable to complete kafka clean up! {}", e.getMessage());
            }
        }
    }
}
