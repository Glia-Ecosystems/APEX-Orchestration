package com.netflix.conductor.contribs.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.contribs.kafka.resource.handlers.ResourceHandler;
import com.netflix.conductor.contribs.kafka.resource.handlers.ResourceHandler.ResponseContainer;
import com.netflix.conductor.core.events.queue.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

/**
 * Class handles the process of filtering the Kafka message for the Conductor API and filtering
 * the response for the client
 *
 * @author Glia Ecosystems
 */
public class KafkaMessageHandler {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageHandler.class);
    private final ResourceHandler resourceHandler;
    private String errorMessage;

    public KafkaMessageHandler(final ResourceHandler resourceHandler){
        this.resourceHandler = resourceHandler;
    }

    /**
     * Main function used to filter the message received from kafka to send to the Conductor API. Also,
     * this function will return the response from the Conductor API as a Message object to be sent via Kafka
     * @param message Message object containing received message from Kafka
     * @return Message object of the response from Conductor API or any errors that may occur during the process
     */
    public Message processMessage(final Message message){
        Map<String, ?> request = jsonStringToMap(message.getPayload());
        if (request == null){
            ResponseContainer responseContainer = new ResponseContainer();
            responseContainer.setResponseErrorMessage(errorMessage);
            responseContainer.setResponseEntity(message.getPayload());
            return new Message(message.getId(), toJSONString(responseContainer), "");
        }
        //Verifies client message for Conductor
        if(requestMessageErrors(request)){
            ResponseContainer responseContainer = new ResponseContainer();
            responseContainer.setResponseErrorMessage(errorMessage);
            responseContainer.setResponseEntity(request);
            return new Message(message.getId(), toJSONString(responseContainer), "");
        }
        // Get the necessary info from the request message for sending to the Conductor API
        String path = resourceHandler.verifyRequestedURIPath((String) request.get("path"));
        String method = resourceHandler.verifyRequestedHTTPMethod((String) request.get("method"));
        Object entity = request.get("request");
        ResponseContainer responseContainer = resourceHandler.processRequest(path, method, entity);
        return new Message(message.getId(), toJSONString(responseContainer), "");
    }

    /**
     * Verifies the payload from the message to verify that necessary information were provided for the Conductor API
     * @param requestMessage Map object of the request message sent to the Conductor API via Kafka
     * @return Indicator of if the message contains all required information.
     */
    public boolean requestMessageErrors(Map<String, ?> requestMessage){
        if (requestMessage.get("path") == null) {
            logger.error("Conductor API request message sent via conductor contain missing/empty URI path", requestMessage);
            errorMessage = "Conductor API request message sent via conductor contain missing/empty URI path";
            return true;
        }else if (requestMessage.get("method") == null) {
            logger.error("Conductor API request message sent via conductor contain missing/empty HTTP method", requestMessage);
            errorMessage = "Conductor API request message sent via conductor contain missing/empty HTTP method";
            return true;
        }
        return false;
    }

    /**
     * Converts a Json string to a Map object
     * @param payload The client request retrieved from Kafka
     * @return Map object of the client request
     */
    private <T> Map<String, T> jsonStringToMap(String payload){
        ObjectMapper mapper = new ObjectMapper();
        Map<String, T> message = null;
        try {
            message = mapper.readValue(payload, new TypeReference<Map<String, T >>() {});
        } catch (JsonProcessingException e) {
            logger.error("Error converting deserialize json to map", e);
            errorMessage = "Error converting deserialize json to map: " + e.toString();
        }
        return message;
    }

    /**
     * Converts an Object to a Json String
     * @param response Object containing the message to be send back to the client
     * @return Json string message
     */
    private String toJSONString(Object response){
        ObjectMapper mapper = new ObjectMapper();
        String responseMessage = null;
        try{
            responseMessage = mapper.writeValueAsString(response);
        } catch (JsonProcessingException e) {
            logger.error("Error converting response message to json", e);
            responseMessage = "Error converting response message to json: " + e.toString();
        }
        return responseMessage;
    }


}
