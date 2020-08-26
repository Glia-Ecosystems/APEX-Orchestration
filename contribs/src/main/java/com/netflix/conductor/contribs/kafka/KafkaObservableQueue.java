package com.netflix.conductor.contribs.kafka;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.netflix.conductor.contribs.kafka.config.KafkaPropertiesProvider;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.core.execution.ApplicationException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
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
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Reads the properties with prefix 'kafka.producer.', 'kafka.consumer.' and 'kafka.' from the
 * provided configuration. Initializes a producer and consumer based on the given value. Queue name
 * is driven from the workflow. It is assumed that the queue name provided is already configured in
 * the kafka cluster.
 *
 * @author Glia Ecosystems
 */
public class KafkaObservableQueue implements ObservableQueue {

    private static final Logger logger = LoggerFactory.getLogger(KafkaObservableQueue.class);
    private static final String QUEUE_TYPE = "kafka";
    private final String queueName;
    private final int pollIntervalInMS;
    private final Duration pollTimeoutInMs;
    private KafkaProducer<String, String> producer;
    private KafkaConsumer<String, String> consumer;


    /**
     * Constructor of KafkaObservableQueue for Conductor Event/System tasks
     *
     * @param queueName Topic for consuming or publishing messages via kafka
     * @param config    Main configuration file for the Conductor application
     */
    @Inject
    public KafkaObservableQueue(final String queueName, final Configuration config,
                                final KafkaPropertiesProvider kafkaPropertiesProvider) {
        this.queueName = queueName;  // Topic
        this.pollIntervalInMS = config.getIntProperty("kafka.consumer.pollingInterval", 1000);
        this.pollTimeoutInMs = Duration.ofMillis(config.getIntProperty("kafka.consumer.longPollTimeout", 1000));
        init(config, kafkaPropertiesProvider);  // Init Kafka producer and consumer properties
    }

    /**
     * Initializes the kafka consumer and producer with the properties of prefix 'kafka.producer.', 'kafka.consumer.'
     * and 'kafka.' from the provided configuration. Queue name (Topic) is provided from the workflow if kafka is
     * initialized in a event queue. It is/should be assumed that the queue name provided is already configured in
     * the kafka cluster. Fails if any mandatory configs are missing.
     *
     * @param config Main configuration file for the Conductor application
     * @param kafkaPropertiesProvider  Filters through the configurations and provide the needed kafka properties
     */
    private void init(final Configuration config, final KafkaPropertiesProvider kafkaPropertiesProvider) {
        // You must set the properties in the .properties files first for creating a producer/consumer object
        final Properties producerProperties = kafkaPropertiesProvider.getProducerProperties();
        final Properties consumerProperties = kafkaPropertiesProvider.getConsumerProperties();
        final String serverId = config.getServerId();
        consumerProperties.put("group.id", queueName + "_group");
        consumerProperties.put("client.id", queueName + "_consumer_" + serverId);
        producerProperties.put("client.id", queueName + "_producer_" + serverId);

        try {
            // Init Kafka producer and consumer
            producer = new KafkaProducer<>(producerProperties);
            consumer = new KafkaConsumer<>(consumerProperties);
            // Assumption is that the consumer topic queueName provided is already configured within the Kafka cluster.
            // This is where Consumer subscribe to given Topic
            consumer.subscribe(Collections.singletonList(queueName));
            logger.info("KafkaObservableQueue initialized for {} topic", queueName);
        } catch (final KafkaException ke) {
            throw new KafkaException("Kafka initialization failed.", ke.getCause());
        }
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
        if (messages == null || messages.isEmpty()) {
            return;
        }
        for (final Message message : messages) {
            final ProducerRecord<String, String> record = new ProducerRecord<>(queueName, message.getId(),
                    message.getPayload());
            final RecordMetadata metadata;
            try {
                metadata = producer.send(record).get();
                final String producerLogging = "Producer Record: key " + record.key() + ", value " + record.value() +
                        ", partition " + metadata.partition() + ", offset " + metadata.offset();
                logger.debug(producerLogging);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Publish message to kafka topic {} failed with an error: {}", queueName, e.getMessage(), e);
            } catch (final ExecutionException e) {
                logger.error("Publish message to kafka topic {} failed with an error: {}", queueName, e.getMessage(), e);
                throw new ApplicationException(ApplicationException.Code.INTERNAL_ERROR, "Failed to publish the event");
            }
        }
        logger.info("Messages published to kafka topic {}. count {}", queueName, messages.size());

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
}
