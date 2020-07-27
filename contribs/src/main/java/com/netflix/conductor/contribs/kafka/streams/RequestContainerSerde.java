package com.netflix.conductor.contribs.kafka.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.contribs.kafka.resource.RequestContainer;
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

    public RequestContainerSerde (final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Configure the underlying serializer and deserializer.
     *
     * @param configs Map object containing the configurations
     * @param isKey Indicator if configurations is for key or value
     */
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {

    }

    /**
     * Close this serde class, which will close the underlying serializer and deserializer.
     * This method has to be idempotent because it might be called multiple times.
     */
    @Override
    public void close() {

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
            public void configure(final Map<String, ?> configs, final boolean isKey) { }

            @Override
            public RequestContainer deserialize(final String topic, final byte[] dataRecord) {
                final Map<String, ?> request = jsonStringToMap(new String(dataRecord));
                // Verifies client message for Conductor
                if (requestMessageErrors(request)) {
                    // Add statement for returning errors to client via kafka streams.
                }
                // Get the necessary info from the request message for sending to the Conductor API
                final String path = verifyRequestedURIPath((String) request.get("path"));
                final String method = verifyRequestedHTTPMethod((String) request.get("method"));
                final Object entity = request.get("request");
                return new RequestContainer(path, method, entity);
            }

            @Override
            public void close() {

            }
        };
    }

    /**
     * Verifies the given URI is of correct syntax
     *
     * @param path Given URI from client request
     * @return URI for requested resource
     */
    public String verifyRequestedURIPath(final String path) {
        return path.startsWith("/") ? path : "/" + path;
    }

    /**
     * Verifies the given HTTP method is capitalized
     *
     * @param httpMethod Given HTTP method from client request
     * @return Upper Case HTTP method
     */
    public String verifyRequestedHTTPMethod(final String httpMethod) {
        return httpMethod.toUpperCase();
    }

    /**
     * Verifies the payload from the message to verify that necessary information were provided for the Conductor API
     * @param requestMessage Map object of the request message sent to the Conductor API via Kafka
     * @return Indicator of if the message contains all required information.
     */
    public boolean requestMessageErrors(final Map<String, ?> requestMessage){
        if (requestMessage.get("path") == null) {
            logger.error("Conductor API request message sent via conductor contain missing/empty URI path");
            return true;
        }else if (requestMessage.get("method") == null) {
            logger.error("Conductor API request message sent via conductor contain missing/empty HTTP method");
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
