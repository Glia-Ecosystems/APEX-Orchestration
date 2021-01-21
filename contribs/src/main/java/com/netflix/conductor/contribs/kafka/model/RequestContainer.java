package com.netflix.conductor.contribs.kafka.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Container object for the client request
 */
public class RequestContainer {

    private final String resourceURI;
    private final String httpMethod;
    private final Object entity;
    // The below fields are used for exception handling in kafka streams during deserialization
    private boolean deserializationErrorOccurred;
    private String deserializationError;

    public RequestContainer(final String resourceURI, final String httpMethod, final Object entity) {
        this.resourceURI = resourceURI;
        this.httpMethod = httpMethod;
        this.entity = entity;
        this.deserializationErrorOccurred = false;
        this.deserializationError = "";
    }

    /**
     * Get the requested URI
     * @return URI
     */
    public String getResourceURI() {
        return resourceURI;
    }

    /**
     * Get the requested HTTP method
     * @return HTTP method
     */
    public String getHttpMethod() {
        return httpMethod;
    }

    /**
     * Get the entity for the request
     *
     * @return Entity
     */
    public Object getEntity() {
        return entity;
    }

    /**
     * Get indication of if an error occurred during deserialization of request
     *
     * @return Indicator if an error occurred or not
     */
    public boolean isDeserializationErrorOccurred() {
        return deserializationErrorOccurred;
    }

    /**
     * Set indication of if an error occurred during deserialization of request
     *
     * @param deserializationErrorOccurred Indicator if an error occurred
     */
    public void setDeserializationErrorOccurred(boolean deserializationErrorOccurred) {
        this.deserializationErrorOccurred = deserializationErrorOccurred;
    }

    /**
     * Get error message that occurred during deserialization of request
     *
     * @return Error message of error that occurred during deserialization of request
     */
    public String getDeserializationError() {
        return deserializationError;
    }

    /**
     * Set error message that occurred during deserialization of request
     *
     * @param deserializationError Error message of error that occurred during deserialization of request
     */
    public void setDeserializationError(String deserializationError) {
        this.deserializationError = deserializationError;
    }

    /**
     * Creates a map containing the field values of Request Container
     *
     * @return Map of the field values of the class
     */
    public Map<String, Object> getRequestData() {
        final Map<String, Object> requestData = new HashMap<>();
        requestData.put("resourceURI", resourceURI);
        requestData.put("httpMethod", httpMethod);
        requestData.put("entity", entity);
        return requestData;
    }

    /**
     * Creates a to String object for printing the requestContainer object
     *
     * @return String object of the class
     */
    @Override
    public String toString() {
        return "RequestContainer{" +
                "resourceURI='" + resourceURI + '\'' +
                ", httpMethod='" + httpMethod + '\'' +
                ", entity=" + entity +
                '}';
    }
}
