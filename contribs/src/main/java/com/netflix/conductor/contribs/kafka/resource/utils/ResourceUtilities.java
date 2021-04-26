package com.netflix.conductor.contribs.kafka.resource.utils;

/**
 * Provides utility functions for loading and managing resources
 */
public class ResourceUtilities {

    private ResourceUtilities() {
    }

    /**
     * Get the classloader for the current thread
     * A classloader is used to load .class files into the JVM at runtime
     *
     * @return the context class loader
     */
    public static ClassLoader getContextClassLoader() {
        return Thread.currentThread().getContextClassLoader();
    }
}

