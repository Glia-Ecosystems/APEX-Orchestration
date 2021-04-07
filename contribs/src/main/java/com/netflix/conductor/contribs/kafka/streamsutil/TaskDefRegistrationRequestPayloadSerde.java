package com.netflix.conductor.contribs.kafka.streamsutil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.contribs.kafka.model.RequestPayload;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Creates a Serdes object for serializing and deserializing Request Payload objects in kafka streams
 *
 * @author Glia Ecosystems
 */
public class TaskDefRegistrationRequestPayloadSerde implements Serde<RequestPayload> {

    private static final Logger logger = LoggerFactory.getLogger(TaskDefRegistrationRequestPayloadSerde.class);
    private static final String TASKDEFREGISTRATIONPATH = "/metadata/taskdefs";
    private static final String TASKDEFREGISTRATIONREQUESTMETHOD = "POST";
    private final ObjectMapper objectMapper;
    private String errorMessage;

    public TaskDefRegistrationRequestPayloadSerde() {
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Configure the underlying serializer and deserializer.
     *
     * @param configs Map object containing the configurations
     * @param isKey Indicator if configurations is for key or value
     */
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // This method is left empty until needed.
    }

    /**
     * Close this serde class, which will close the underlying serializer and deserializer.
     * This method has to be idempotent because it might be called multiple times.
     */
    @Override
    public void close() {
        // This method is left empty until needed.
    }

    /**
     * Creates the serializer for this class.
     * @return A Request Payload Serializer
     */
    @Override
    public Serializer<RequestPayload> serializer() {
        return null;
    }

    /**
     * Creates the deserializer for this class.
     * @return A Request Payload Deserializer
     */
    @Override
    public Deserializer<RequestPayload> deserializer() {
        return new Deserializer<RequestPayload>() {
            @Override
            public void configure(final Map<String, ?> configs, final boolean isKey) {
                // This method is left empty until needed.
            }

            /**
             * Handles the deserialization of a record received from Kafka to a RequestPayload object
             *
             * @param topic The topic the record was consumed from
             * @param dataRecord The record received from topic
             * @return Request Payload object of the record received
             */
            @Override
            public RequestPayload deserialize(final String topic, final byte[] dataRecord) {
                final Map<String, ?> request = jsonStringToMap(new String(dataRecord));
                errorMessage = "";
                // Verify if record is null
                if (request == null){
                    // If record sent is null
                    errorMessage = "Task Def Registration API request message received is Null";
                    RequestPayload requestPayload = new RequestPayload("", "", "", null);
                    requestPayload.setDeserializationErrorOccurred(true);
                    requestPayload.setDeserializationError(errorMessage);
                    return requestPayload;
                }
                // Verifies client message for Conductor
                if (requestPayloadErrors(request)) {
                    final String key = (String) request.get("key");
                    RequestPayload requestPayload = new RequestPayload(key, "", "", request.get("request"));
                    requestPayload.setDeserializationErrorOccurred(true);
                    requestPayload.setDeserializationError(errorMessage);
                    return requestPayload;
                }
                // Get the necessary info from the request message for sending to the Task Def Registration API
                final String key = (String) request.get("key");
                final Object entity = request.get("request");
                return new RequestPayload(key, TASKDEFREGISTRATIONPATH, TASKDEFREGISTRATIONREQUESTMETHOD, entity);
            }

            @Override
            public void close() {
                // This method is left empty until needed.
            }
        };
    }

    /**
     * Verifies that the necessary information were provided in the payload for the Task Def Registration API
     * @param requestPayload Map object of the request payload sent to the Task Def Registration API
     * @return Indication if the payload contains all required information.
     */
    private boolean requestPayloadErrors(final Map<String, ?> requestPayload){
        if (requestPayload.get("key") == null || requestPayload.get("key") == "") {
            errorMessage = "Task Def Registration API request payload contain missing/empty key";
            logger.error("Task Def Registration API request payload contain missing/empty key");
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
        }
        return message;
    }
}

