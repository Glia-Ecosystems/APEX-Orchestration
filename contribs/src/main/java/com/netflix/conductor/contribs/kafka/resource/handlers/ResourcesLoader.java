package com.netflix.conductor.contribs.kafka.resource.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.*;

public class ResourcesLoader {

    private static final Logger logger = LoggerFactory.getLogger(ResourcesLoader.class);
    private final ApplicationContext applicationContext;
    private final Set<Class<?>> classes;
    private final String resourcesPath;

    /**
     * Initialization of the ResourceLoader class
     *
     * @param resourcesPath The relative path to the resource package
     */
    public ResourcesLoader(final ApplicationContext applicationContext, final String resourcesPath) {
        this.classes = new LinkedHashSet<>();
        this.applicationContext = applicationContext;
        this.resourcesPath = resourcesPath;
    }

    public void locateResources() {
        String[] resources = applicationContext.getBeanNamesForType(Object.class);
        for (String resource: resources) {
            Class<?> bean = applicationContext.getType(resource);
            // AnnotatedElementUtils.hasAnnotation(bean, RequestMapping.class)
            if (bean != null && bean.getPackageName().equals(resourcesPath) &&
                    bean.isAnnotationPresent(RequestMapping.class)) {
                    classes.add(bean);
            }
        }
    }

    public Set<Class<?>> getClasses() {
        return classes;
    }
}
