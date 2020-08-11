package com.netflix.conductor.contribs.kafka.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.netflix.conductor.contribs.kafka.KafkaStreamsObservableQueue;
import com.netflix.conductor.contribs.kafka.resource.handlers.ResourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Monitors and updates the client via kafka the status of the workflow until
 * completion of workflow
 *
 * @author Glia Ecosystems
 */
public class WorkflowStatusMonitor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowStatusMonitor.class);
    private static final Gson gson = new Gson();
    private final ResourceHandler resourceHandler;
    private final ObjectMapper objectMapper;
    private final KafkaStreamsObservableQueue kafka;
    private final String clientID;
    private final String workflowID;
    private Object currentStatus;

    public WorkflowStatusMonitor(final ResourceHandler resourceHandler, final ObjectMapper objectMapper,
                                 final KafkaStreamsObservableQueue kafkaStreams, final String clientID,
                                 final String workflowID) {
        this.resourceHandler = resourceHandler;
        this.objectMapper = objectMapper;
        this.kafka = kafkaStreams;
        this.clientID = clientID;
        this.workflowID = workflowID;
        this.currentStatus = "";
    }

    /**
     * Makes a request to conductor to get the status of the workflow
     *
     * @return Response from the resource contained in a response container object
     */
    private ResponseContainer requestWorkflowStatus(){
        String path = "/workflow/" + workflowID;
        return resourceHandler.processRequest(new RequestContainer(path, "GET", ""));
    }

    /**
     * Makes a request to conductor to retry the last failed task in the current running
     * workflow
     */
    private void retryLastTask(){
        String path = "/workflow/" + workflowID + "/retry";
        resourceHandler.processRequest(new RequestContainer(path, "POST", ""));
    }

    /**
     * Main method of the class for monitoring the lifecycle of a workflow
     */
    private void monitor() {
        boolean completed = false;
        while (!completed) {
            ResponseContainer responseContainer = requestWorkflowStatus();
            Map<String, Object> workflow = objectToMap(responseContainer.getResponseEntity());
            Object workflowStatus = workflow.get("status");
            clientUpdateVerifier(responseContainer, workflowStatus);
            completed = (workflowStatus == "COMPLETED") || (workflowStatus == "TERMINATED");
            pollingInterval();
        }
    }

    /**
     * Verify and update the client when the status of a workflow has changed
     *
     * @param responseContainer Response object for sending all needed information about the response from the Conductor API
     * @param workflowStatus Status of the workflow
     */
    private void clientUpdateVerifier(final ResponseContainer responseContainer, final Object workflowStatus) {
        if (!workflowStatus.equals(currentStatus)) {
            currentStatus = workflowStatus;
            updateClientOfWorkFlowStatus(responseContainer);
            // Retry the last task in workflow for the client if a workflow status "FAILED" is received
            if (workflowStatus == "FAILED") {
                retryLastTask();
            }
        }
    }

    /**
     * Send the status update of the workflow to the client via kafka
     *
     * @param responseContainer Response object for sending all needed information about the response from the Conductor API
     */
    private void updateClientOfWorkFlowStatus(final ResponseContainer responseContainer) {
        kafka.publishMessage(clientID, gson.toJson(responseContainer.getResponseData()));
    }

    /**
     * Convert a generic object to a map object
     *
     * @param object root class object
     * @return Map object of the given object
     */
    private Map<String, Object> objectToMap(final Object object) {
        return objectMapper.convertValue(object, new TypeReference<Map<String, Object>>() {});
    }

    /**
     * Execute a Thread sleep for the established time
     */
    private void pollingInterval(){
        // Thread.sleep function is executed so that a consumed message is not sent
        // to Conductor before the server is started
        try {
            Thread.sleep(10); // 10 millisecond thread sleep
        } catch (final InterruptedException e) {
            // Restores the interrupt by the InterruptedException so that caller can see that
            // interrupt has occurred.
            Thread.currentThread().interrupt();
            logger.error("Error occurred while trying to sleep Thread. {}", e.getMessage());
        }
    }

    /**
     * Main method for executing
     */
    @Override
    public void run() {
        try {
            monitor();
        } catch (final Exception e) {
            logger.error("WorkflowStatusMonitor.monitor(), exiting due to error! {}", e.getMessage());
        }
    }
}
