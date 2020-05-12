package com.netflix.conductor.contribs.kafka.resource.builder;

import javax.ws.rs.Path;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Resource class object of a resource class
 * Contains all the information needed for use of the resource
 *
 * @author Glia Ecosystems
 *
 */
public class Resource {

    private final Class<?> resource;
    private final Path pathURI;
    private final List<Field> fields;
    private final List<Constructor<?>> constructors;
    private final Map<String, List<ResourceMethod>> methods;

    public Resource(Class<?> resource, Path uri) {
        this.resource = resource;
        this.pathURI = uri;
        this.fields = new ArrayList<>();
        this.constructors = new ArrayList<>();
        this.methods = new HashMap<>();
    }

    /**
     * Get URI path
     * @return URI
     */
    public Path getPathURI() {
        return pathURI;
    }

    /**
     * Get Resource class
     * @return Resource class
     */
    public Class<?> getResource() {
        return resource;
    }

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
