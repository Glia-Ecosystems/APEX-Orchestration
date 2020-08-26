package com.netflix.conductor.contribs.kafka.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.contribs.kafka.config.KafkaPropertiesProvider;
import com.netflix.conductor.core.config.Configuration;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WorkersTaskStreamFactory {

    private final Set<String> activeWorkers;
    private final ExecutorService threadPool;
    private final KafkaPropertiesProvider kafkaPropertiesProvider;
    private final ObjectMapper objectMapper;

    public WorkersTaskStreamFactory(final Configuration configuration,
                                    final KafkaPropertiesProvider kafkaPropertiesProvider,
                                    final ObjectMapper objectMapper) {
        this.activeWorkers = new LinkedHashSet<>();
        this.objectMapper = objectMapper;
        this.kafkaPropertiesProvider = kafkaPropertiesProvider;
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
     *
     * @param responseContainer
     */
    public void processWorker(final ResponseContainer responseContainer){
        Map<String, Object> request = responseContainer.getRequest();
        ArrayList<?> entity = (ArrayList<?>) request.get("entity");
        TaskDef taskDef = objectMapper.convertValue(entity.get(0), TaskDef.class);
        String workerName = taskDef.getName();
        addActiveWorker(workerName);
    }
}
