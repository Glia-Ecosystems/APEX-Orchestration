package com.netflix.conductor.contribs.kafka.resource.handlers;


/**
 * Provides utility functions for loading and managing resources
 */
public class ResourceUtilities {

    /**
     * Get the classloader for the current thread
     * A classloader is used to load .class files into the JVM at runtime
     *
     * @return the context class loader
     */
    public ClassLoader getContextClassLoader() {
        return Thread.currentThread().getContextClassLoader();
    }
}





