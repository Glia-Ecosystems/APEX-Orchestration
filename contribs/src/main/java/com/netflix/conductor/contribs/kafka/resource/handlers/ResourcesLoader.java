package com.netflix.conductor.contribs.kafka.resource.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Path;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ResourcesLoader {

    private static final Logger logger = LoggerFactory.getLogger(ResourcesLoader.class);
    private final Set<Class<?>> classes;
    private final ClassLoader classloader;
    private final String resourcesPath;
    private final ResourceUtilities resourceUtilities;
    private final Set<String> annotations;

    /**
     * Implements ResourceLoader Constructor chaining by getting the context classloader
     * and passing it with all given arguments to the main initialization constructor
     * @param resourcesPath The relative path to the resource package
     * @param resourceUtilities An object that provides helper functions for managing resources
     */
    public ResourcesLoader(final String resourcesPath, ResourceUtilities resourceUtilities) {
        this(resourceUtilities.getContextClassLoader(), resourcesPath, resourceUtilities);
    }

    /**
     * Initialization of the ResourceLoader class
     * @param classLoader A object that is part of the JRE for dynamically loading classes
     * @param resourcesPath The relative path to the resource package
     * @param resourceUtilities An object that provides helper functions for managing resources
     */
    private ResourcesLoader(final ClassLoader classLoader, final String resourcesPath, ResourceUtilities resourceUtilities) {
        this.resourceUtilities = resourceUtilities;
        this.resourcesPath = resourcesPath;
        this.classes = new LinkedHashSet<Class<?>>();
        this.classloader = classLoader;
        this.annotations = Stream.of("L" + Path.class.getName().replaceAll("\\.", "/") + ";")
                .collect(Collectors.toCollection(HashSet::new));
    }

    /**
     * Loads the requested resource jar file to be processed
     */
    public void locateResources() {
        try {
            final Enumeration<URL> urls = classloader.getResources(resourcesPath.replace('.', '/'));
            while (urls.hasMoreElements()) {
                readJarFile(urls.nextElement());
            }
        } catch (IOException ex) {
            logger.error("Can not locate Resources for Kafka Listener", ex);
        }
    }

    /**
     * Creates a InputStream to be used for reading the given Jar file.
     * @param url URL object of the Jar File to be read
     */
    public void readJarFile(final URL url) {
        // Parses the file absolute path for relevant information
        final String file = url.getFile();
        final String jarFileUrl = file.substring(0, file.lastIndexOf('!'));
        final String resourceParentPath = file.substring(file.lastIndexOf('!') + 2);
        // Implements a try-with resource statement that will automatically close resources upon statement
        // complete or if abrupt as a result of an error
        try (BufferedInputStream inputStream = new BufferedInputStream(new URL(jarFileUrl).openStream())) {
            read(inputStream, resourceParentPath);
        } catch (IOException ex) {
            logger.error("Error attempting to scan the JarFile: " + jarFileUrl, ex);
        }
    }

    /**
     * Creates a JarInputStream for reading the Jar File and scan the jar file to get the
     * classes from the resource file for processing
     * @param inputStream A BufferedInputStream object for reading chunks of bytes
     * @param parent The parent path of the resource
     * @throws IOException
     */
    private void read(final InputStream inputStream, final String parent) throws IOException {
        try (JarInputStream jarInputStream = new JarInputStream(inputStream)) {
            JarEntry entry = jarInputStream.getNextJarEntry();
            while (entry != null) {
                if (!entry.isDirectory() && entry.getName().startsWith(parent) && entry.getName().endsWith(".class")) {
                    // Process found file
                }
                jarInputStream.closeEntry();
                entry = jarInputStream.getNextJarEntry();
            }
        }
    }
}

