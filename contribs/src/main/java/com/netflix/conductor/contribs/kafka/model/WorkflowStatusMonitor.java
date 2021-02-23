package com.netflix.conductor.contribs.kafka.model;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.netflix.conductor.contribs.kafka.resource.handlers.ResourceHandler;
import com.netflix.conductor.core.execution.ApplicationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Monitors and updates the client via kafka the status of the workflow until
 * completion of workflow
 *
 * @author Glia Ecosystems
 */
public class WorkflowStatusMonitor implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(WorkflowStatusMonitor.class);
    private final Gson gson;
    private final ResourceHandler resourceHandler;
    private final ObjectMapper objectMapper;
    private final KafkaProducer<String, String> producer;
    private final String clientID;
    private final String workflowID;
    private final String responsesTopic;
    private final int workflowStatusPollingInterval;
    private Object currentStatus;


    public WorkflowStatusMonitor(final ResourceHandler resourceHandler, final ObjectMapper objectMapper,
                                 final KafkaProducer<String, String> producer, final String responsesTopic,
                                 final String clientID, final String workflowID, final int workflowStatusPollingInterval) {
        this.resourceHandler = resourceHandler;
        this.objectMapper = objectMapper;
        this.gson = new Gson();
        this.producer = producer;
        this.responsesTopic = responsesTopic;
        this.clientID = clientID;
        this.workflowID = workflowID;
        this.workflowStatusPollingInterval = workflowStatusPollingInterval;
        this.currentStatus = "";
    }

    /**
     * Makes a request to conductor to get the status of the workflow
     *
     * @return Response from the resource contained in a response container object
     */
    private ResponseContainer requestWorkflowStatus(){
        String path = "/workflow/" + workflowID;
        return resourceHandler.processRequest(new RequestContainer(clientID, path, "GET", ""));
    }

    /**
     * Makes a request to conductor to retry the last failed task in the current running
     * workflow
     */
    private void retryLastTask(){
        String path = "/workflow/" + workflowID + "/retry";
        resourceHandler.processRequest(new RequestContainer(clientID, path, "POST", ""));
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
        publishStatusMessage(gson.toJson(responseContainer.getResponseData()));
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
     * Publish the messages to the given topic.
     *
     * @param value Value of the record to be publish
     */
    public void publishStatusMessage(final String value) {
        final ProducerRecord<String, String> record = new ProducerRecord<>(responsesTopic, value);
        final RecordMetadata metadata;
        try {
            metadata = producer.send(record).get();
            final String producerLogging = "Producer Record: key " + record.key() + ", value " + record.value() +
                    ", partition " + metadata.partition() + ", offset " + metadata.offset();
            logger.debug(producerLogging);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Publish message to kafka topic {} failed with an error: {}", responsesTopic, e.getMessage(), e);
        } catch (final ExecutionException e) {
            logger.error("Publish message to kafka topic {} failed with an error: {}", responsesTopic, e.getMessage(), e);
            throw new ApplicationException(ApplicationException.Code.INTERNAL_ERROR, "Failed to publish the event");
        }
        logger.info("Message published to kafka topic {}. key/client {}", responsesTopic, clientID);
    }

    /**
     * Execute a Thread sleep for the established time
     */
    private void pollingInterval(){
        // Thread.sleep function is executed so that a consumed message is not sent
        // to Conductor before the server is started
        try {
            Thread.sleep(workflowStatusPollingInterval);
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
