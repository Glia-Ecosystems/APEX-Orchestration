package com.netflix.conductor.contribs.kafka.resource.builder;

import org.springframework.web.bind.annotation.RequestMapping;

import javax.ws.rs.Path;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Resource {

    private final Class<?> resourceClass;
    private final RequestMapping pathURI;
    private final List<Field> fields;
    private final List<Constructor<?>> constructors;
    private final Map<String, List<ResourceMethod>> methods;

    public Resource(final Class<?> resourceClass, final RequestMapping uri) {
        this.resourceClass = resourceClass;
        this.pathURI = uri;
        this.fields = new ArrayList<>();
        this.constructors = new ArrayList<>();
        this.methods = new HashMap<>();
    }

    /**
     * Get URI path
     *
     * @return URI
     */
    public RequestMapping getPathURI() {
        return pathURI;
    }

    /**
     * Get Resource class
     *
     * @return Resource class
     */
    public Class<?> getResourceClass() {
        return resourceClass; }

    /**
     * Get list of resource fields
     * @return Resource fields
     */
    public List<Field> getFields() {
        return fields;
    }

    /**
     * Get list of the resource constructors
     * @return Resource constructors
     */
    public List<Constructor<?>> getConstructors() {
        return constructors;
    }

    /**
     * Get list of resource methods
     * @return Resource methods
     */
    public Map<String, List<ResourceMethod>> getMethods() {
        return methods;
    }
}
