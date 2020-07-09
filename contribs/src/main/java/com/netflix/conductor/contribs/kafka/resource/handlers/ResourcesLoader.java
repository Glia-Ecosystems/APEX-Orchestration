package com.netflix.conductor.contribs.kafka.resource.handlers;

import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.ClassReader;
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
    private final Set<String> annotations;
    private final AnnotatedClassesVisitor classVisitor;

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
        this.annotations = Stream.of("L" + Path.class.getName().replaceAll("\\.", "/") + ";")
                .collect(Collectors.toCollection(HashSet::new));
        this.classVisitor = new AnnotatedClassesVisitor();
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
        } catch (final IOException ex) {
            logger.error("Can not locate Resources for Kafka Listener. {}", ex.getMessage());
        }
    }

    /**
     * Creates a InputStream to be used for reading the given Jar file.
     * @param url URL object of the Jar File to be read
     */
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

    /**
     * Creates a JarInputStream for reading the Jar File and uses ASM library, which is
     * an all purpose java bytecode analysis and manipulation framework, for scanning
     * the jar files for the necessary resource classes.
     * @param inputStream A BufferedInputStream object for reading chunks of bytes
     * @param parent The parent path of the resource
     * @throws IOException Indicator that an input/output exception have occurred
     */
    private void read(final InputStream inputStream, final String parent) throws IOException {
        try (final JarInputStream jarInputStream = new JarInputStream(inputStream)) {
            JarEntry entry = jarInputStream.getNextJarEntry();
            while (entry != null) {
                if (!entry.isDirectory() && entry.getName().startsWith(parent) && entry.getName().endsWith(".class")) {
                    // ClassReader assist the ClassVisitor by reading the file.
                    new ClassReader(jarInputStream).accept(classVisitor, 0);
                }
                jarInputStream.closeEntry();
                entry = jarInputStream.getNextJarEntry();
            }
        }
    }

    /**
     * Getter method for the Set collection of loaded Resource instances
     * @return Set of Resource classes
     */
    public Set<Class<?>> getClasses() {
        return classes;
    }

    /**
     * The ClassVisitor class have all the methods to access the components of a given
     * Java class. This subclass provides extended functions for verifying if a class is
     * annotated and if so, load the class.
     */
    private final class AnnotatedClassesVisitor extends ClassVisitor {

        private String className;
        private boolean isScoped;
        private boolean isClassAnnotated;

        /**
         * Initialize the ClassVisitor class with the ASM API version.
         *
         * Opcodes defines the Java Virtual Machine opcodes, access flags,
         * and array type codes.
         */
        public AnnotatedClassesVisitor() {
            super(Opcodes.ASM5);
        }

        /**
         * Provides the functionality to visit the header of the class
         *
         * @param version    The class version
         * @param access     The class access flags
         * @param name       The name of the given class
         * @param signature  The signature of the class if its not a generic class or implements or extends generics
         * @param superName  The name of the given class associated super class, if exist
         * @param interfaces The name of the given class associated interface classes, if exist
         */
        @Override
        public void visit(final int version, final int access, final String name, final String signature, final String superName, final String[] interfaces) {
            className = name;
            // ACC_PUBLIC provides the access flag for public classes, fields, and methods
            isScoped = (Opcodes.ACC_PUBLIC & access) != 0;
            isClassAnnotated = false;
        }

        /**
         * Visits the annotation, if present, in a Java class and checks if its an annotation of interest,
         * then set isClassAnnotated to True to indicate that the given class is of interest.
         *
         * @param desc    The descriptor of the annotation class
         * @param visible Indicator of if the annotation is visible at runtime
         * @return Null
         */
        @Override
        public AnnotationVisitor visitAnnotation(final String desc, final boolean visible) {
            isClassAnnotated = annotations.contains(desc) || isClassAnnotated;
            return null;
        }

        /**
         * The visitEnd method is the last method called in the ClassVisitor process to indicate that
         * all the fields and methods of the given class as been visited.
         * <p>
         * This function is extended to load the class into the classes Map if it is a resource class
         * of interest for processing client requests for Conductor
         */
        @Override
        public void visitEnd() {
            if (isScoped && isClassAnnotated) {
                classes.add(loadResourceClass(className.replace("/", ".")));
            }
        }

        /**
         * Load an instance of the class specified by the given class name
         *
         * Added a suppressed warning because a Type safety: Unchecked cast error occur when
         * casting Class.forName return type Class<?> to  Class<T>. I believe the cast
         * is needed because the class type reference matters
         * @param className The name of the class to be loaded
         * @return An instance of the requested class to be loaded
         */
        @SuppressWarnings("unchecked")
        private <T> Class<T> loadResourceClass(final String className) {
            try {
                return (Class<T>) Class.forName(className, false, classloader);
            } catch (final ClassNotFoundException ex) {
                final String error = "The class file " + className + " is identified as an annotated class but could not be found";
                // Throwing a generic runtime exception
                throw new RuntimeException(error, ex);
            }
        }
    }
}

