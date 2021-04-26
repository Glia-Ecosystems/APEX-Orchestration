package com.netflix.conductor.contribs.kafka.resource.handlers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles the resources for processing client requests to the Conductor API
 *
 * @author Glia Ecosystems
 */
public class ResourceHandler {

    private static final Logger logger = LoggerFactory.getLogger(ResourceHandler.class);
    private final ResourcesLoader resourcesLoader;
    //private final Map<String, Resource> resourceMap = new HashMap<>();
    private final ObjectMapper objectMapper;

    public ResourceHandler(final ObjectMapper objectMapper, final ResourcesLoader resourcesLoader) {
        this.resourcesLoader = resourcesLoader;
        this.objectMapper = objectMapper;
        init();
    }

    public void init(){
        // Load resources from resource package path
        resourcesLoader.locateResources();

    }
}
