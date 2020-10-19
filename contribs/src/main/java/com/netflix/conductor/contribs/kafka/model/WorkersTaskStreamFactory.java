package com.netflix.conductor.contribs.kafka.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.contribs.kafka.config.KafkaPropertiesProvider;
import com.netflix.conductor.contribs.kafka.resource.handlers.ResourceHandler;
import com.netflix.conductor.core.config.Configuration;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WorkersTaskStreamFactory {

    private static final String QUEUED_TASKS_TOPIC = "Task-Queue";
    private static final String UPDATE_AND_ACK_TOPIC = "Update-Ack";
    private static final String UPDATE_AND_ACK_RESPONSE_TOPIC = "Update-Ack-Response";
    private final Set<String> activeWorkers;
    private final ExecutorService threadPool;
    private final KafkaTopicsManager kafkaTopicManager;
    private final KafkaPropertiesProvider kafkaPropertiesProvider;
    private final Properties producerProperties;
    private final int pollBatchSize;
    private final ResourceHandler resourceHandler;
    private final ObjectMapper objectMapper;

    public WorkersTaskStreamFactory(final Configuration configuration, final KafkaPropertiesProvider kafkaPropertiesProvider,
                                    final ResourceHandler resourceHandler, final ObjectMapper objectMapper) {
        this.activeWorkers = new LinkedHashSet<>();
        this.objectMapper = objectMapper;
        this.resourceHandler = resourceHandler;
        this.kafkaPropertiesProvider = kafkaPropertiesProvider;
        this.kafkaTopicManager = new KafkaTopicsManager(configuration, kafkaPropertiesProvider);
        this.producerProperties = kafkaPropertiesProvider.getProducerProperties();
        this.pollBatchSize = configuration.getIntProperty("conductor.kafka.workers.listener.poll.batch.size", 30);
        this.threadPool = Executors.newFixedThreadPool(configuration.getIntProperty("conductor.kafka.workers.listener.thread.pool", 30));
    }

    /**
     * Creates or destroy a worker task stream object/thread
     *
     * @param  worker The name of the worker
     * @param responseContainer Response object for sending all needed information about the response from the Conductor API
     */
    public ResponseContainer createOrDestroyWorkerTaskStream(final String worker, final ResponseContainer responseContainer) {
        Map<String, Object> request = responseContainer.getRequest();
        String httpMethod = (String) request.get("httpMethod");
        if (httpMethod.equals("DELETE")) {
            removeInActiveWorker(worker);
        } else {
            ArrayList<?> entity = (ArrayList<?>) request.get("entity");
            TaskDef taskDef = objectMapper.convertValue(entity.get(0), TaskDef.class);
            String uniqueWorkerName = setUniqueKey(worker);  // Set a unique service name
            addActiveWorker(uniqueWorkerName);  // Adds worker to active workers collection
            Map<String, String> topics = createTopics(worker);  // Create topics for service to communicate with Conductor

            // Return unique service name and topics to service
            Map<String, Object> registrationResponse = new HashMap<>();
            registrationResponse.put("uniqueKey", uniqueWorkerName);
            registrationResponse.put("topics", topics);
            responseContainer.setResponseEntity(registrationResponse);

            // Start worker task stream
            createWorkerTaskStream(uniqueWorkerName, taskDef.getName(), topics);
        }
        return responseContainer;
    }

    /**
     * Creates a worker task stream for processing tasks between Conductor and the respective service
     * @param workerName The unique service name
     * @param taskName The name of the task definition
     * @param topics A list of topics used for Conductor and service to communicate
     */
    private void createWorkerTaskStream(final String workerName, final String taskName, final Map<String, String> topics){
        Properties responseStreamProperties =kafkaPropertiesProvider.getStreamsProperties("response-" + workerName);
        threadPool.execute(new WorkerTasksStream(resourceHandler, responseStreamProperties, producerProperties,
                activeWorkers, workerName, taskName, topics, pollBatchSize));

    }

    /**
     * Create the generated service topics on the Kafka cluster
     * @param worker The name of the worker
     * @return  A list of topics used for Conductor and service to communicate
     */
    private Map<String, String> createTopics(final String worker){
        Map<String, String> topics = generateServiceTopics(worker);
        kafkaTopicManager.createTopics(new ArrayList<>(topics.values()));
        return topics;
    }

    /**
     * Set a unique key for a service to communicate with Conductor via Kafka
     * This assist with Conductor being able to communicate with multiple instances of a service and
     * know when a particular instance is no longer running (communicating with Conductor) so that
     * thread (Worker Task Stream) can be closed
     * @param worker The name of the worker
     * @return A unique key
     */
    private String setUniqueKey(String worker){
        if (activeWorkers.contains(worker)){
            int occurrences = activeWorkerFrequency(activeWorkers, worker);
            return worker + occurrences;
        }
        return worker;
    }

    /**
     * Add the name (or unique key) of an active worker to the active workers collection
     *
     * @param workerName The unique (key) service name
     */
    private void addActiveWorker(String workerName){
        activeWorkers.add(workerName);
    }

    /**
     * Generates topics for Conductor to communicate with the respective service for processing tasks
     * @param worker The name of the worker
     * @return List of generated topics for the service
     */
    private Map<String, String> generateServiceTopics(final String worker){
        Map<String, String> topics = new HashMap<>();
        topics.put("taskQueue", worker + "-" + QUEUED_TASKS_TOPIC);
        topics.put("updateAndAck", worker + "-" + UPDATE_AND_ACK_TOPIC);
        topics.put("updateAndAckResponse", worker + "-" + UPDATE_AND_ACK_RESPONSE_TOPIC);
        return topics;
    }

    /**
     * Removes an inactive service from the active workers collection
     *
     * @param workerName The name of the worker
     */
    private void removeInActiveWorker(String workerName){
        activeWorkers.remove(workerName);
    }

    /**
     * Checks the number of instances of a service currently registered to Conductor
     * @param collection A collection object
     * @param worker The name of the worker
     * @return The number of instances already running
     */
    private int activeWorkerFrequency(final Collection<String> collection, final String worker){
        int occurrence = 0;
        for (String activeWorker: collection){
            if (activeWorker.contains(worker)){
                occurrence += 1;
            }
        }
        return occurrence;
    }
}
