package com.netflix.conductor.contribs.kafka.resource.handlers;

public class ResourceHandler {

    private String RESOURCE_PATH = "com.netflix.conductor.server.resources";

    private final ResourcesLoader resourcesLoader;
    private final ResourceUtilities util;

    public ResourceHandler() {
        this.util = new ResourceUtilities();
        this.resourcesLoader = new ResourcesLoader(RESOURCE_PATH, util);
        init();
    }

    private void init() {
        resourcesLoader.locateResources();
        // Build resource map for processing client requests
        int i = 1;
    }

    public void processRequest(){
        // Create a request container
        // Process request
        // Create response container
        // Return response
    }
}
