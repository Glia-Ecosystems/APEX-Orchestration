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

    private final List<String> activeWorkers;
    private final ExecutorService threadPool;
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
        this.producerProperties = kafkaPropertiesProvider.getProducerProperties();
        this.pollBatchSize = configuration.getIntProperty("conductor.kafka.workers.listener.poll.batch.size", 30);
        this.threadPool = Executors.newFixedThreadPool(configuration.getIntProperty("conductor.kafka.workers.listener.thread.pool", 30));
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

    /**
     * Creates or destroy a worker task stream object/thread
     *
     * @param responseContainer Response object for sending all needed information about the response from the Conductor API
     */
    public void createOrDestroyWorkerTaskStream(final String worker, final ResponseContainer responseContainer) {
        Map<String, Object> request = responseContainer.getRequest();
        String httpMethod = (String) request.get("httpMethod");
        if (httpMethod.equals("DELETE")) {
            removeInActiveWorker(worker);
        } else {
            ArrayList<?> entity = (ArrayList<?>) request.get("entity");
            TaskDef taskDef = objectMapper.convertValue(entity.get(0), TaskDef.class);
            addActiveWorker(worker);
            Properties responseStreamProperties =kafkaPropertiesProvider.getStreamsProperties("response-" + worker);
            threadPool.execute(new WorkerTasksStream(resourceHandler, responseStreamProperties, producerProperties,
                    activeWorkers, worker, taskDef.getName(), pollBatchSize));
        }
    }
}
