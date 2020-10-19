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

    private static final String TASKS_TOPIC = "Task-Queue";
    private static final String UPDATE_TOPIC = "Update";
    private static final String STATUS_TOPIC = "Status";
    private final Map<String, Integer> activeWorkers;
    private final ExecutorService threadPool;
    private final KafkaTopicsManager kafkaTopicManager;
    private final KafkaPropertiesProvider kafkaPropertiesProvider;
    private final Properties producerProperties;
    private final int pollBatchSize;
    private final ResourceHandler resourceHandler;
    private final ObjectMapper objectMapper;

    public WorkersTaskStreamFactory(final Configuration configuration, final KafkaPropertiesProvider kafkaPropertiesProvider,
                                    final ResourceHandler resourceHandler, final ObjectMapper objectMapper) {
        this.activeWorkers = new HashMap<>();
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
    public ResponseContainer createWorkerTaskStream(final String worker, final ResponseContainer responseContainer) {
        Map<String, Object> request = responseContainer.getRequest();
        ArrayList<?> entity = (ArrayList<?>) request.get("entity");
        TaskDef taskDef = objectMapper.convertValue(entity.get(0), TaskDef.class);
        addActiveWorker(worker);  // Adds worker to active workers collection
        Map<String, String> topics = createTopics(worker);  // Create topics for service to communicate with Conductor

        // When the first instance of a service is registered, create and start the worker task stream
        if (activeWorkers.get(worker) == 1){
            createWorkerTaskStream(worker, taskDef.getName(), topics);
        }
        // Return topics to service
        responseContainer.setResponseEntity(topics);
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
     * Add the name of an active worker to the active workers collection and
     * set the number of instances of a worker currently running.
     *
     * This assist with Conductor knowing when a particular service is no longer
     * running (communicating with Conductor) so that thread (Worker Task Stream) can be closed
     * @param worker The unique (key) service name
     */
    private void addActiveWorker(final String worker) {
        if (!activeWorkers.containsKey(worker)) {
            activeWorkers.put(worker, 1);
        } else {
            int frequency = activeWorkers.get(worker) + 1;
            activeWorkers.put(worker, frequency);
        }
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
        topics.put("statusTopic", worker + "-" + STATUS_TOPIC);
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
}
