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

    public RequestContainer(final String resourceURI, final String httpMethod, final Object entity) {
        this.resourceURI = resourceURI;
        this.httpMethod = httpMethod;
        this.entity = entity;
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
