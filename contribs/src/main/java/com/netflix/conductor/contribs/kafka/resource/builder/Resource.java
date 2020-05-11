package com.netflix.conductor.contribs.kafka.resource.builder;

import javax.ws.rs.Path;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Resource {

    private final Class<?> resource;
    private final Path pathURI;
    private final List<Field> fields;
    private final List<Constructor> constructors;
    private final Map<String, List<ResourceMethod>> methods;

    public Resource(Class<?> resource, Path uri) {
        this.resource = resource;
        this.pathURI = uri;
        this.fields = new ArrayList<Field>();
        this.constructors = new ArrayList<Constructor>();
        this.methods = new HashMap<String, List<ResourceMethod>>();
    }


    public Path getPathURI() {
        return pathURI;
    }
    public List<Field> getFields() {
        return fields;
    }

    public Class<?> getResource() {
        return resource;
    }

    public List<Constructor> getConstructors() {
        return constructors;
    }

    public Map<String, List<ResourceMethod>> getMethods() {
        return methods;
    }
}
