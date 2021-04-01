package com.netflix.conductor.contribs.kafka.streamsutil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.contribs.kafka.model.RequestContainer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

/**
 * Creates a Serdes object for serializing and deserializing Request Container objects in kafka streams
 *
 * @author Glia Ecosystems
 */
public class RequestContainerSerde implements Serde<RequestContainer> {

    private static final Logger logger = LoggerFactory.getLogger(RequestContainerSerde.class);
    private final ObjectMapper objectMapper;
    private String errorMessage;

    public RequestContainerSerde () {
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
     * @return A Request Container Serializer
     */
    @Override
    public Serializer<RequestContainer> serializer() {
        return null;
    }

    /**
     * Creates the deserializer for this class.
     * @return A Request Container Deserializer
     */
    @Override
    public Deserializer<RequestContainer> deserializer() {
        return new Deserializer<RequestContainer>() {
            @Override
            public void configure(final Map<String, ?> configs, final boolean isKey) {
                // This method is left empty until needed.
            }

            /**
             * Handles the deserialization of a record received from Kafka to a RequestContainer object
             *
             * @param topic The topic the record was consumed from
             * @param dataRecord The record received from topic
             * @return Request Container object of the record received
             */
            @Override
            public RequestContainer deserialize(final String topic, final byte[] dataRecord) {
                errorMessage = "";
                final Map<String, ?> request = jsonStringToMap(new String(dataRecord));
                // Verify if record is null
                if (request == null){
                    // If record sent is null
                    errorMessage = "Conductor API request message sent via conductor is Null";
                    RequestContainer requestContainer = new RequestContainer("", "", "", null);
                    requestContainer.setDeserializationErrorOccurred(true);
                    requestContainer.setDeserializationError(errorMessage);
                    return requestContainer;
                }
                // Verifies client message for Conductor
                if (requestMessageErrors(request)) {
                    final String key = (String) request.get("key");
                    final String path = (String) request.get("path");
                    final String method = (String) request.get("method");
                    RequestContainer requestContainer = new RequestContainer(key, path, method, request.get("request"));
                    requestContainer.setDeserializationErrorOccurred(true);
                    requestContainer.setDeserializationError(errorMessage);
                    return requestContainer;
                }
                // Get the necessary info from the request message for sending to the Conductor API
                final String key = (String) request.get("key");
                final String path = verifyRequestedURIPath((String) request.get("path"));
                final String method = verifyRequestedHTTPMethod((String) request.get("method"));
                final Object entity = request.get("request");
                return new RequestContainer(key, path, method, entity);
            }

            @Override
            public void close() {
                // This method is left empty until needed.
            }
        };
    }

    /**
     * Verifies the given URI is of correct syntax
     *
     * @param path Given URI from client request
     * @return URI for requested resource
     */
    private String verifyRequestedURIPath(final String path) {
        return path.startsWith("/") ? path : "/" + path;
    }

    /**
     * Verifies the given HTTP method is capitalized
     *
     * @param httpMethod Given HTTP method from client request
     * @return Upper Case HTTP method
     */
    private String verifyRequestedHTTPMethod(final String httpMethod) {
        return httpMethod.toUpperCase();
    }

    /**
     * Verifies the payload from the message to verify that necessary information were provided for the Conductor API
     * @param requestMessage Map object of the request message sent to the Conductor API via Kafka
     * @return Indicator of if the message contains all required information.
     */
    private boolean requestMessageErrors(final Map<String, ?> requestMessage){
        if (requestMessage.get("key") == null || requestMessage.get("key") == "") {
            errorMessage = "Conductor API request message sent via kafka contain missing/empty key";
            logger.error("Conductor API request message sent via kafka contain missing/empty key");
            return true;
        } else if (requestMessage.get("path") == null || requestMessage.get("path") == "") {
            errorMessage = "Conductor API request message sent via kafka contain missing/empty URI path";
            logger.error("Conductor API request message sent via kafka contain missing/empty URI path");
            return true;
        }else if (requestMessage.get("method") == null || requestMessage.get("method") == "") {
            errorMessage = "Conductor API request message sent via kafka contain missing/empty HTTP method";
            logger.error("Conductor API request message sent via kafka contain missing/empty HTTP method");
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
