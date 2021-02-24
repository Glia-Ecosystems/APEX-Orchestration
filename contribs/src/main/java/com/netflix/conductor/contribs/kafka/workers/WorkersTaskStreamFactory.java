package com.netflix.conductor.contribs.kafka.workers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.contribs.kafka.config.KafkaPropertiesProvider;
import com.netflix.conductor.contribs.kafka.model.KafkaTopicsManager;
import com.netflix.conductor.contribs.kafka.model.ResponseContainer;
import com.netflix.conductor.contribs.kafka.resource.handlers.ResourceHandler;
import com.netflix.conductor.core.config.Configuration;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WorkersTaskStreamFactory {

    private static final String TASKS_TOPIC = "Task-Queue";
    private static final String UPDATE_TOPIC = "Update";
    private final ActiveWorkersMonitor activeWorkersMonitor;
    private final ExecutorService threadPool;
    private final KafkaTopicsManager kafkaTopicManager;
    private final KafkaPropertiesProvider kafkaPropertiesProvider;
    private final Properties producerProperties;
    private final int pollBatchSize;
    private final int taskPollingInterval;
    private final ResourceHandler resourceHandler;
    private final ObjectMapper objectMapper;

    public WorkersTaskStreamFactory(final Configuration configuration, final KafkaPropertiesProvider kafkaPropertiesProvider,
                                    final ActiveWorkersMonitor activeWorkersMonitor, final KafkaTopicsManager kafkaTopicsManager,
                                    final ResourceHandler resourceHandler, final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.resourceHandler = resourceHandler;
        this.kafkaPropertiesProvider = kafkaPropertiesProvider;
        this.kafkaTopicManager = kafkaTopicsManager;
        this.activeWorkersMonitor = activeWorkersMonitor;
        this.producerProperties = kafkaPropertiesProvider.getProducerProperties();
        this.pollBatchSize = configuration.getIntProperty("conductor.kafka.workers.listener.poll.batch.size", 30);
        this.taskPollingInterval = configuration.getIntProperty("conductor.kafka.workers.task.stream.task.polling.interval", 1);
        this.threadPool = Executors.newFixedThreadPool(configuration.getIntProperty("conductor.kafka.workers.listener.thread.pool", 30));
    }

    /**
     * Creates a worker task stream object/thread
     *
     * @param  worker The name of the worker
     * @param responseContainer Response object for sending all needed information about the response from the Conductor API
     */
    public ResponseContainer createWorkerTaskStream(final String worker, final ResponseContainer responseContainer) {
        Map<String, String> topics = createTopics(worker);  // Create topics for service to communicate with Conductor

        // When the first instance of a service is registered, create and start the worker task stream
        if (!activeWorkersMonitor.isActive(worker)){
            Map<String, Object> request = responseContainer.getRequest();
            ArrayList<?> entity = (ArrayList<?>) request.get("entity");
            TaskDef taskDef = objectMapper.convertValue(entity.get(0), TaskDef.class);
            activeWorkersMonitor.addActiveWorker(worker, taskDef.getName());  // Adds worker to active workers collection
            startWorkerTaskStream(worker, taskDef.getName(), topics);
        }
        // Return topics to service
        responseContainer.setResponseEntity(topics);
        return responseContainer;
    }

    /**
     * Starts a worker task stream for processing tasks between Conductor and the respective service
     * @param workerName The unique service name
     * @param taskName The name of the task definition
     * @param topics A list of topics used for Conductor and service to communicate
     */
    private void startWorkerTaskStream(final String workerName, final String taskName, final Map<String, String> topics){
        Properties responseStreamProperties = kafkaPropertiesProvider.getStreamsProperties("response-" + workerName);
        threadPool.execute(new WorkerTasksStream(resourceHandler, responseStreamProperties, producerProperties,
                activeWorkersMonitor, workerName, taskName, topics, pollBatchSize, taskPollingInterval));

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
     * Generates topics for Conductor to communicate with the respective service for processing tasks
     * @param worker The name of the worker
     * @return List of generated topics for the service
     */
    private Map<String, String> generateServiceTopics(final String worker){
        Map<String, String> topics = new HashMap<>();
        topics.put("taskTopic", worker + "-" + TASKS_TOPIC);
        topics.put("updateTopic", worker + "-" + UPDATE_TOPIC);
        return topics;
    }

    /**
     * Verifies if there are workers/services registered to Conductor already upon start up of
     * the server. If so, start task streams for workers.
     *
     * NOTE - This function should only be used once upon start up of the server
     */
    public void verifyExistingWorkersAndCreateTaskStreams(){
        List<TaskDef> taskDefs = activeWorkersMonitor.getExistingTaskDefinitions();
        if (!taskDefs.isEmpty()){
            for (TaskDef taskDef: taskDefs){
                // Capitalize worker name
                String worker = taskDef.getName().substring(0, 1).toUpperCase() + taskDef.getName().substring(1);
                Map<String, String> topics = generateServiceTopics(worker);
                activeWorkersMonitor.addActiveWorker(worker, taskDef.getName());  // Adds worker to active workers collection
                startWorkerTaskStream(worker, taskDef.getName(), topics);
            }
        }

    }
}
