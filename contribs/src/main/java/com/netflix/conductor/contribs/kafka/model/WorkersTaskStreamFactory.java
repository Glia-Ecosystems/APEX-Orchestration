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
    private final List<String> activeWorkers;
    private final ExecutorService threadPool;
    private final KafkaTopicsManager kafkaTopicManager;
    private final KafkaPropertiesProvider kafkaPropertiesProvider;
    private final Properties producerProperties;
    private final int pollBatchSize;
    private final ResourceHandler resourceHandler;
    private final ObjectMapper objectMapper;

    public WorkersTaskStreamFactory(final Configuration configuration, final KafkaPropertiesProvider kafkaPropertiesProvider,
                                    final ResourceHandler resourceHandler, final ObjectMapper objectMapper) {
        this.activeWorkers = new ArrayList<>();
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
            addActiveWorker(worker);  // Adds worker to active workers collection
            Map<String, String> topics = createTopics(worker);  // Create topics for service to communicate with Conductor
            responseContainer.setResponseEntity(topics);  // Return topics to service
            createWorkerTaskStream(worker, taskDef.getName(), topics);  // Start worker task stream
        }
        return responseContainer;
    }

    private void createWorkerTaskStream(final String worker, final String taskName, final Map<String, String> topics){
        Properties responseStreamProperties =kafkaPropertiesProvider.getStreamsProperties("response-" + worker);
        threadPool.execute(new WorkerTasksStream(resourceHandler, responseStreamProperties, producerProperties,
                activeWorkers, worker, taskName, topics, pollBatchSize));

    }

    private Map<String, String> createTopics(final String worker){
        Map<String, String> topics = new HashMap<>();
        topics.put("taskQueue", worker + "-" + QUEUED_TASKS_TOPIC);
        topics.put("updateAndAck", worker + "-" + UPDATE_AND_ACK_TOPIC);
        topics.put("updateAndAckResponse", worker + "-" + UPDATE_AND_ACK_RESPONSE_TOPIC);
        kafkaTopicManager.createTopics(new ArrayList<>(topics.values()));
        return topics;
    }

    /**
     * Add an active worker to the active workers collection
     *
     * @param workerName The task name of the worker
     */
    private void addActiveWorker(String workerName){
        activeWorkers.add(workerName);
    }

    /**
     * Removes an inactive worker from the active workers collection
     *
     * @param workerName The task name of the worker
     */
    private void removeInActiveWorker(String workerName){
        activeWorkers.remove(workerName);
    }
}
