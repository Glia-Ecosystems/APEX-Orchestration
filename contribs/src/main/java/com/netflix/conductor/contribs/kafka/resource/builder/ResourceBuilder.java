package com.netflix.conductor.contribs.kafka.resource.builder;

import com.netflix.conductor.contribs.kafka.resource.builder.ResourceMethod.MethodParameter;
import com.netflix.conductor.contribs.kafka.resource.builder.ResourceMethod.ParameterSource;

import javax.ws.rs.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;

public class ResourceBuilder {

    public static Resource buildResource(final Resource resource) {
        // Works out what the resource fields are and adds them to Resource object
        resourceFields(resource);
        // Works out what the resource constructor is and add it to Resource object
        resourceConstructors(resource);
        // Works out what the resource methods are and adds them to Resource object
        resourceMethods(resource);

        return resource;
    }

    private static void resourceConstructors(final Resource resource){
        Class<?> resourceClass = resource.getResource();
        final Constructor[] constructors = resourceClass.getConstructors();
        if (constructors != null){
            for (Constructor con: constructors){
                resource.getConstructors().add(con);
            }
        }
    }

    private static void resourceFields(final Resource resource) {
        Class<?> resourceClass = resource.getResource();
        final Field[] fields = resourceClass.getDeclaredFields();
        for (Field field: fields){
            resource.getFields().add(field);
        }
    }

    private static void resourceMethods(final Resource resource){
        Class<?> resourceClass = resource.getResource();
        final Method[] methods = resourceClass.getMethods();
        for (Method method: methods) {
            for (Annotation annotation: method.getAnnotations()){
                if (annotation.annotationType().getAnnotation(HttpMethod.class) != null){
                    Class<?> returnType = method.getReturnType();
                    Annotation[] annotations = method.getAnnotations();
                    String httpMethod = annotation.annotationType().getAnnotation(HttpMethod.class).value();
                    ResourceMethod resourceMethod =  new ResourceMethod(method, method.getAnnotation(Path.class),
                            httpMethod, annotations, returnType);
                    addMethodToResource(resource, resourceMethod, httpMethod);
                    resourceMethodParameters(resourceMethod, method);
                }
            }
        }
    }

    private static void addMethodToResource(final Resource resource, final ResourceMethod resourceMethod,
                                            final String httpMethod){
        Map<String, List<ResourceMethod>> methodMap = resource.getMethods();
        if (methodMap.get(httpMethod) != null){
            methodMap.get(httpMethod).add(resourceMethod);
        } else {
            methodMap.put(httpMethod, new ArrayList<>(Collections.singletonList(resourceMethod)));
        }

    }

    private static void resourceMethodParameters(final ResourceMethod resourceMethod, final Method method) {
        Class[] parameterTypes = method.getParameterTypes();
        Type[] genericParameterTypes = method.getGenericParameterTypes();
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();

        for (int i = 0; i < parameterTypes.length; i++) {
            MethodParameter parameter = resourceMethodParameter(parameterTypes[i],
                    genericParameterTypes[i], parameterAnnotations[i]);
            resourceMethod.getParameters().add(parameter);
        }
    }
    private static MethodParameter resourceMethodParameter(final Class<?> parameterClass, final Type parameterType ,
                                                           final Annotation[] parameterAnnotations) {
        Annotation parameterAnnotation = null;
        ParameterSource parameterSource = null;
        String parameterName = null;

        for (Annotation annotation : parameterAnnotations) {
            if (PathParam.class == annotation.annotationType()) {
                parameterAnnotation = annotation;
                parameterSource = ParameterSource.PATH;
                parameterName = ((PathParam) annotation).value();
            } else if (QueryParam.class == annotation.annotationType()) {
                parameterAnnotation = annotation;
                parameterSource = ParameterSource.QUERY;
                parameterName = ((QueryParam) annotation).value();
            } else {
                parameterSource = ParameterSource.REGULAR;
            }
        }
        if (parameterAnnotation == null) {
            parameterSource = ParameterSource.REGULAR;
        }
        return new MethodParameter(parameterClass, parameterType, parameterAnnotation, parameterSource, parameterName);
    }
}
