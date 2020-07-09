package com.netflix.conductor.contribs.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.contribs.kafka.resource.WorkflowStatusMonitor;
import com.netflix.conductor.contribs.kafka.resource.handlers.ResourceHandler;
import com.netflix.conductor.contribs.kafka.resource.handlers.ResourceHandler.ResponseContainer;
import com.netflix.conductor.core.events.queue.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

/**
 * Class handles the process of filtering the Kafka message for the Conductor API and filtering
 * the response for the client
 *
 * @author Glia Ecosystems
 */
public class KafkaMessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageHandler.class);
    private final ResourceHandler resourceHandler;
    private final ObjectMapper objectMapper;
    private String errorMessage;

    public KafkaMessageHandler(final ResourceHandler resourceHandler, final ObjectMapper objectMapper) {
        this.resourceHandler = resourceHandler;
        this.objectMapper = objectMapper;
    }

    /**
     * Main function used to filter the message received from kafka to send to the Conductor API and return
     * the response from the Conductor API as a Message object to be sent via Kafka. Also,
     * this function will start a workflow status monitor thread for updating the client via kafka
     * of the status of the started workflow by the client
     * @param message Message object containing received message from Kafka
     * @return Message object of the response from Conductor API or any errors that may occur during the process
     */
    public Message processMessage(final Message message, final KafkaObservableQueue kafka, final ExecutorService pool,
                                  final CountDownLatch countDownLatch){
        final Map<String, ?> request = jsonStringToMap(message.getPayload());
        if (request == null) {
            final ResponseContainer responseContainer = new ResponseContainer();
            responseContainer.setResponseErrorMessage(errorMessage);
            responseContainer.setResponseEntity(message.getPayload());
            return new Message(message.getId(), toJSONString(responseContainer.getResponseData()), "");
        }
        // Verifies client message for Conductor
        if (requestMessageErrors(request)) {
            final ResponseContainer responseContainer = new ResponseContainer();
            responseContainer.setResponseErrorMessage(errorMessage);
            responseContainer.setResponseEntity(request);
            return new Message(message.getId(), toJSONString(responseContainer.getResponseData()), "");
        }
        // Get the necessary info from the request message for sending to the Conductor API
        final String path = resourceHandler.verifyRequestedURIPath((String) request.get("path"));
        final String method = resourceHandler.verifyRequestedHTTPMethod((String) request.get("method"));
        final Object entity = request.get("request");
        final ResponseContainer responseContainer = resourceHandler.processRequest(path, method, entity);
        Message response = new Message(message.getId(), toJSONString(responseContainer.getResponseData()), "");

        // If a workflow was started, create a separate thread to monitor and update the client via kafka of the workflow status
        if (responseContainer.isStartedAWorkflow() && responseContainer.getStatus() == 200){
            String workflowId = (String) responseContainer.getResponseEntity();
            if (!workflowId.equals("")) {
                // Use a thread from the thread pool for monitoring workflow status
                pool.execute(new WorkflowStatusMonitor(resourceHandler, objectMapper, kafka, workflowId, countDownLatch));
            }
        }
        return response;
    }


    /**
     * Verifies the payload from the message to verify that necessary information were provided for the Conductor API
     * @param requestMessage Map object of the request message sent to the Conductor API via Kafka
     * @return Indicator of if the message contains all required information.
     */
    public boolean requestMessageErrors(final Map<String, ?> requestMessage){
        if (requestMessage.get("path") == null) {
            logger.error("Conductor API request message sent via conductor contain missing/empty URI path");
            errorMessage = "Conductor API request message sent via conductor contain missing/empty URI path";
            return true;
        }else if (requestMessage.get("method") == null) {
            logger.error("Conductor API request message sent via conductor contain missing/empty HTTP method");
            errorMessage = "Conductor API request message sent via conductor contain missing/empty HTTP method";
            return true;
        }
        return false;
    }

    /**
     * Converts a Json string to a Map object
     *
     * @param payload The client request retrieved from Kafka
     * @return Map object of the client request
     */
    private Map<String, Object> jsonStringToMap(final String payload) {
        Map<String, Object> message = null;
        try {
            message = objectMapper.readValue(payload, new TypeReference<Map<String, Object>>() {
            });
        } catch (final JsonProcessingException e) {
            logger.error("Error converting deserialize json to map. {}", e.getMessage());
            errorMessage = "Error converting deserialize json to map: " + e.toString();
        }
        return message;
    }

    /**
     * Converts an Object to a Json String
     *
     * @param response Object containing the message to be sent back to the client
     * @return Json string message
     */
    private String toJSONString(final Object response) {
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


}
