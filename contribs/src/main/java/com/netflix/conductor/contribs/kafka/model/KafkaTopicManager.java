package com.netflix.conductor.contribs.kafka.model;

import com.netflix.conductor.contribs.kafka.config.KafkaProperties;
import com.netflix.conductor.contribs.kafka.config.KafkaPropertiesProvider;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.ProviderException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class KafkaTopicsManager {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTopicsManager.class);
    private final AdminClient adminClient;
    private final int requestTimeoutMS;
    private final int numOfPartitions;
    private final short numOfReplications;

    public KafkaTopicsManager(final KafkaProperties configuration, final KafkaPropertiesProvider kafkaPropertiesProvider){
        this.requestTimeoutMS = configuration.getIntProperty("topic.manager.kafka.request.timeout.ms", 10000);
        this.numOfPartitions = configuration.getIntProperty("topic.manager.kafka.num.of.partitions", 3);
        this.numOfReplications = (short) configuration.getIntProperty("topic.manager.kafka.num.of.replications", 1);
        adminClient = AdminClient.create(kafkaPropertiesProvider.getAdminProperties());
    }

    /**
     * Attempts to create a topic in Kafka using the given topicName, if there is no topic with this
     * same topicName. Nothing will happen if the topic already exists. The AdminClient instance
     * created with this class will be used to check for existing topics using the method AdminClient.listTopics().
     *
     * @param topicName The unique name of a topic to be created in Kafka.
     */
    public void createTopic(final String topicName) {
        try {
            if (!topicExists(topicName)) {
                final NewTopic aTopic = new NewTopic(topicName, numOfPartitions, numOfReplications);
                final CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(aTopic));
                // returns a future so we should block so we know it's been done
                result.all().get(Long.parseLong(String.valueOf(requestTimeoutMS)), TimeUnit.MILLISECONDS);
                logger.info("Topic {} created", topicName);
            } else {
                logger.info("Topic {} already exists, no need to create it", topicName);
            }
        } catch (final Exception e) {
            throw new ProviderException("KafkaTopicManager.createTopic()." + topicName + " " + e.toString(), e);
        }
    }

    /**
     * Attempts to create topics in Kafka using the given topics in the list provided, if the topic do not
     * already exist. Nothing will happen if the topic already exists. The AdminClient
     * created with this class will be used to check for existing topics using the method AdminClient.listTopics().
     *
     * @param topics A list containing the unique names of topics to be created in Kafka.
     */
    public void createTopics(final List<String> topics) {
        try {
            List<String> topicsNotExist = topicExists(topics);
            if (!topicsNotExist.isEmpty()) {
                for (String topicName: topicsNotExist){
                    final NewTopic aTopic = new NewTopic(topicName, numOfPartitions, numOfReplications);
                    final CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(aTopic));
                    // returns a future so we should block so we know it's been done
                    result.all().get(Long.parseLong(String.valueOf(requestTimeoutMS)), TimeUnit.MILLISECONDS);
                    logger.info("Topic {} created", topicName);
                }
            }
        } catch (final Exception e) {
            throw new ProviderException("KafkaTopicManager.createTopic()." + topics + " " + e.toString(), e);
        }
    }


    /**
     * Attempts to delete a topic in Kafka using the given topicName, if there is no topic with this
     * same topicName. Nothing will happen if the topic does not exist.
     *
     * @param topicName The unique name of a topic to be deleted.
     */
    public void deleteTopic(final String topicName) {
        try {
            if (topicExists(topicName)) {
                final KafkaFuture<Void> result = adminClient.deleteTopics(Collections.singletonList(topicName)).all();
                result.get(Long.parseLong(String.valueOf(requestTimeoutMS)), TimeUnit.MILLISECONDS);
                logger.info("KafkaTopicManager.deleteTopic() removed topic {} isDone {} isCancelled {} exception {}",
                        topicName, result.isDone(), result.isCancelled(), result.isCompletedExceptionally());
            }
        } catch (final Exception e) {
            throw new ProviderException("KafkaTopicManager.deleteTopic()." + topicName + " " + e.toString(), e);
        }
    }

    /**
     * Check if the topic exist
     *
     * @param topic the topic name
     * @return true/false if the topic exists
     */
    private boolean topicExists(final String topic) {
        try {
            final Set<String> existingTopics = adminClient.listTopics().names().get();
            return !existingTopics.isEmpty() && existingTopics.contains(topic);
        } catch (final Exception e) {
            throw new ProviderException("KafkaTopicManager.topicExists()." + topic + " " + e.toString(), e);
        }
    }

    /**
     * Check if the list of topics exists
     *
     * @param topics A list containing the unique names of topics to be created in Kafka.
     * @return List of topics that do not exist
     */
    private List<String> topicExists(final List<String> topics) {
        try {
            final Set<String> existingTopics = adminClient.listTopics().names().get();
            if (!existingTopics.isEmpty()) {
                // Loop through topics requested to be created and check if any already exist
                List<String> topicsNotExist = new ArrayList<>();
                for (String topicName : topics) {
                    if (!existingTopics.contains(topicName)) {
                        topicsNotExist.add(topicName);
                    } else {
                        logger.info("Topic {} already exists, no need to create it", topicName);
                    }
                }
                // Return only topics that do not exist already
                return topicsNotExist;
            } else {
                // No topics exist on the broker.
                return topics;
            }
        } catch (final Exception e) {
            throw new ProviderException("KafkaTopicManager.topicExists()." + topics + " " + e.toString(), e);
        }
    }

    /**
     * Closes connection to Kafka Admin API on Kafka cluster.
     */
    public void close() {
        try {
            logger.info("KafkaTopicManager.close() cleaning up topics and consumer groups");
            adminClient.close();
        } catch (final Exception e) {
            throw new ProviderException("KafkaTopicManager.close() " + e.toString(), e);
        }
    }
}
