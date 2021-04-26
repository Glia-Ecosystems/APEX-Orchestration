package com.netflix.conductor.contribs.kafka.resource.handlers;

import com.netflix.conductor.contribs.kafka.resource.utils.ResourceUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.LinkedHashSet;
import java.util.Set;

public class ResourcesLoader {

    private static final Logger logger = LoggerFactory.getLogger(ResourcesLoader.class);
    private final Set<Class<?>> classes;
    private final ClassLoader classloader;
    private final String resourcesPath;
    // private final Set<String> annotations;
    //private final AnnotatedClassesVisitor classVisitor;

    /**
     * Implements ResourceLoader Constructor chaining by getting the context classloader
     * and passing it with all given arguments to the main initialization constructor
     *
     * @param resourcesPath The relative path to the resource package
     */
    public ResourcesLoader(final String resourcesPath) {
        this(ResourceUtilities.getContextClassLoader(), resourcesPath);
    }

    /**
     * Initialization of the ResourceLoader class
     *
     * @param classLoader   A object that is part of the JRE for dynamically loading classes
     * @param resourcesPath The relative path to the resource package
     */
    private ResourcesLoader(final ClassLoader classLoader, final String resourcesPath) {
        this.resourcesPath = resourcesPath;
        this.classes = new LinkedHashSet<>();
        this.classloader = classLoader;
        //this.annotations = .of("L" + Path.class.getName()
        //        .replaceAll("\\.", "/") + ";")
        //        .collect(Collectors.toCollection(HashSet::new));
        //this.classVisitor = new AnnotatedClassesVisitor();
    }

    public void locateResources() {
        try{
            final Enumeration<URL> urls = classloader.getResources(resourcesPath.replace('.', '/'));
            while (urls.hasMoreElements()){
                readJarFile(urls.nextElement());
            }
        } catch (IOException e) {
            logger.error("Can not locate Resources for Kafka Listener. {}", e.getMessage());
        }
    }

    private void readJarFile(final URL url) {
        // Parses the file absolute path for relevant information
        final String file = url.getFile();
        final String jarFileUrl = file.substring(0, file.lastIndexOf('!'));
        final String resourceParentPath = file.substring(file.lastIndexOf('!') + 2);
        // Implements a try-with resource statement that will automatically close resources upon statement
        // complete or if abrupt as a result of an error
        try (final BufferedInputStream inputStream = new BufferedInputStream(new URL(jarFileUrl).openStream())) {
            read(inputStream, resourceParentPath);
        } catch (final IOException ex) {
            logger.error("Error attempting to scan the JarFile {} {}", jarFileUrl, ex.getMessage());
        }
    }

    private void read(final InputStream inputStream, final String parent) throws IOException {
        int a = 1;
    }
}
