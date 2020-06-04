package com.netflix.conductor.contribs.kafka.resource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.contribs.kafka.KafkaObservableQueue;
import com.netflix.conductor.contribs.kafka.resource.handlers.ResourceHandler;
import com.netflix.conductor.contribs.kafka.resource.handlers.ResourceHandler.ResponseContainer;
import com.netflix.conductor.core.events.queue.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Monitors and updates the client via kafka the status of the workflow until
 * completion of workflow
 *
 * @author Glia Ecosystems
 */
public class WorkflowStatusMonitor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowStatusMonitor.class);
    private final ResourceHandler resourceHandler;
    private final ObjectMapper objectMapper;
    private final KafkaObservableQueue kafka;
    private final String workflowID;
    private Object currentStatus;

    public WorkflowStatusMonitor(final ResourceHandler resourceHandler, final ObjectMapper objectMapper,
                                 final KafkaObservableQueue kafka, final String workflowID) {
        this.resourceHandler = resourceHandler;
        this.objectMapper = objectMapper;
        this.kafka = kafka;
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
        return resourceHandler.processRequest(path, "GET", "");
    }

    /**
     * Main method of the class for monitoring the lifecycle of a workflow
     */
    private void monitor() {
        boolean completed = false;
        while (!completed) {
            pollingInterval();
            ResponseContainer responseContainer = requestWorkflowStatus();
            Map<String, Object> workflow = objectToMap(responseContainer.getResponseEntity());
            Object workflowStatus = workflow.get("status");
            clientUpdateVerifier(responseContainer, workflowStatus);
            completed = (workflowStatus == "COMPLETED") || (workflowStatus == "TERMINATED");
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
        }
    }

    /**
     * Send the status update of the workflow to the client via kafka
     *
     * @param responseContainer Response object for sending all needed information about the response from the Conductor API
     */
    private void updateClientOfWorkFlowStatus(final ResponseContainer responseContainer) {
        List<Message> responseMessage = new ArrayList<>();
        responseMessage.add(new Message(workflowID, jsonTOString(responseContainer.getResponseData()), ""));
        kafka.publish(responseMessage);
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
     * Converts an Object to a Json String
     *
     * @param response Object containing the message to be sent back to the client
     * @return Json string message
     */
    private String jsonTOString(final Object response) {
        final String responseMessage;
        try {
            responseMessage = objectMapper.writeValueAsString(response);
        } catch (final JsonProcessingException ex) {
            // Check if we want to throw this exception
            String error = "Error converting response message to json. Error: " + ex.getMessage() + " Cause: "
                    + ex.getCause();
            logger.error(error);
            throw new RuntimeException("Error converting response message to json", ex.getCause());
        }
        return responseMessage;
    }

    /**
     * Execute a Thread sleep for the established time
     */
    private void pollingInterval(){
        // Thread.sleep function is executed so that a consumed message is not sent
        // to Conductor before the server is started
        try {
            Thread.sleep(1000); // 1 second thread sleep
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
