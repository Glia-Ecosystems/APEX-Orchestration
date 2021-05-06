package com.netflix.conductor.contribs.kafka.resource.builder;

import com.netflix.conductor.contribs.kafka.resource.builder.ResourceMethod.MethodParameter;
import com.netflix.conductor.contribs.kafka.resource.builder.ResourceMethod.MethodParameter.ParameterAnnotationType;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import javax.ws.rs.*;
import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;

public class ResourceBuilder {

    /**
     * This allows the class to not be instantiated as an object
     */
    private ResourceBuilder() {
    }

    /**
     * Builds the resource object by using reflection to get needed information
     * from the resource class
     *
     * @param resource Contains information about a resource
     * @return Completed resource object of the needed information of a resource class
     */
    public static Resource buildResource(final Resource resource) {
        // Works out what the resource fields are and adds them to Resource object
        resourceFields(resource);
        // Works out what the resource constructor is and add it to Resource object
        resourceConstructors(resource);
        // Works out what the resource methods are and adds them to Resource object
        resourceMethods(resource);

        return resource;
    }

    /**
     * Uses reflection to get the constructors of a resource class
     * @param resource Contains information about a resource
     */
    private static void resourceConstructors(final Resource resource){
        final Class<?> resourceClass = resource.getResourceClass();
        // Gets an array of all the public constructors of the class
        final Constructor<?>[] constructors = resourceClass.getConstructors();
        for (final Constructor<?> constructor : constructors) {
            resource.getConstructors().add(constructor);
        }
    }

    /**
     * Uses reflection to get the fields of a resource class
     * @param resource Contains information about a resource
     */
    private static void resourceFields(final Resource resource) {
        final Class<?> resourceClass = resource.getResourceClass();
        // Gets an array of all the public fields declared in the class
        final Field[] fields = resourceClass.getFields();
        for (final Field field : fields) {
            resource.getFields().add(field);
        }
    }

    /**
     * Uses reflection to get the methods and its parameters of a  resource class
     * @param resource Contains information about a resource
     */
    private static void resourceMethods(final Resource resource) {
        final Class<?> resourceClass = resource.getResourceClass();
        // Gets an array of all the public methods of the class
        final Method[] methods = resourceClass.getDeclaredMethods();
        //final Method[] methods = resourceClass.getMethods();
        for (final Method method : methods) {
            for (final Annotation annotation : method.getDeclaredAnnotations()) {
                if (annotation.annotationType().getAnnotation(RequestMapping.class) != null) {
                    // Gets the class object of the return type
                    final Class<?> returnType = method.getReturnType();
                    // Gets an array of all the method annotations in declaring order
                    final Annotation[] annotations = method.getAnnotations();
                    // Gets a string representation of the HTTP annotation of the method
                    final RequestMapping requestMethod = annotation.annotationType().getAnnotation(RequestMapping.class);
                    final RequestMethod[] dg = requestMethod.method();
                    RequestMapping re = AnnotatedElementUtils.findMergedAnnotation(method, RequestMapping.class);
                    assert re != null;
                    final String httpMethod = re.path()[0];
                    //final String httpMethod = annotation.annotationType().getAnnotation(HttpMethod.class).value();
                    final ResourceMethod resourceMethod = new ResourceMethod(method, method.getAnnotation(RequestMapping.class),
                            httpMethod, annotations, returnType);
                    addMethodToResource(resource, resourceMethod, httpMethod);
                    resourceMethodParameters(resourceMethod, method);
                }
            }
        }
    }

    /**
     * Used to construct/populate the method map
     *
     * Method map: Key: HTTP method, Value: [Related methods to the HTTP method]
     * ex: "POST" -> [post method, post method, ..., ...]
     * @param resource Contains information about a resource
     * @param resourceMethod Contains information about a method
     * @param httpMethod The HTTP annotation of the method
     */
    private static void addMethodToResource(final Resource resource, final ResourceMethod resourceMethod,
                                            final String httpMethod){
        final Map<String, List<ResourceMethod>> methodMap = resource.getMethods();
        if (methodMap.get(httpMethod) != null){
            methodMap.get(httpMethod).add(resourceMethod);
        } else {
            methodMap.put(httpMethod, new ArrayList<>(Collections.singletonList(resourceMethod)));
        }

    }

    /**
     * Uses reflection to get information about the parameters of a method
     * @param resourceMethod Contains information about a method
     * @param method The method of the resource class
     */
    private static void resourceMethodParameters(final ResourceMethod resourceMethod, final Method method) {
        // Gets an array of the class object of the parameter type in declaration order
        final Class<?>[] parameterTypes = method.getParameterTypes();
        // Gets an array of the type object of the parameter type in declaration order
        final Type[] genericParameterTypes = method.getGenericParameterTypes();
        // Gets an array of arrays of any annotations of the parameters in declaration order
        final Annotation[][] parameterAnnotations = method.getParameterAnnotations();

        for (int i = 0; i < parameterTypes.length; i++) {
            final MethodParameter parameter = resourceMethodParameterAnnotations(parameterTypes[i],
                    genericParameterTypes[i], parameterAnnotations[i]);
            resourceMethod.getParameters().add(parameter);
        }
    }

    /**
     * Establishes if there are parameters annotations and create a method parameter object
     * containing the relevant information of a parameter
     *
     * @param parameterClass Class object of the parameter type
     * @param parameterType Type object of the parameter type
     * @param parameterAnnotations Annotation object of the parameter
     * @return MethodParameter object containing all the information about a method parameter
     */
    private static MethodParameter resourceMethodParameterAnnotations(final Class<?> parameterClass, final Type parameterType ,
                                                                      final Annotation[] parameterAnnotations) {
        Annotation parameterAnnotation = null;
        ParameterAnnotationType parameterAnnotationType = null;
        String parameterName = null;
        String parameterDefaultValue = null;
        Method queryParamValueConverter = null;

        for (final Annotation annotation : parameterAnnotations) {
            if (PathParam.class == annotation.annotationType()) {
                parameterAnnotation = annotation;
                parameterAnnotationType = ParameterAnnotationType.PATH;
                parameterName = ((PathParam) annotation).value();
            } else if (QueryParam.class == annotation.annotationType()) {
                parameterAnnotation = annotation;
                parameterAnnotationType = ParameterAnnotationType.QUERY;
                parameterName = ((QueryParam) annotation).value();
                // Get value of method for converting query param from string type
                // to required type for resource method
                if (parameterClass != String.class && parameterClass != List.class) {
                    queryParamValueConverter = getQueryParamStringValueOfMethod(parameterClass);
                }
            } else if (DefaultValue.class == annotation.annotationType()) {
                parameterDefaultValue = ((DefaultValue) annotation).value();
            }
        }
        if (parameterAnnotation == null) {
            parameterAnnotationType = ParameterAnnotationType.ENTITY;
        }
        return new MethodParameter(parameterClass, parameterType, parameterAnnotation, parameterAnnotationType,
                parameterDefaultValue, parameterName, queryParamValueConverter);
    }

    /**
     * Get the string value method with respect to type
     * @param parameterClass Class object of the parameter type
     * @return Value of method for respective type class
     */
    private static Method getQueryParamStringValueOfMethod(final Class<?> parameterClass) {
        // Creates a primitive type map to match type class with actual type
        final Map<Class<?>, Class<?>> primitiveTypes = new WeakHashMap<>();
        primitiveTypes.put(Boolean.TYPE, Boolean.class);
        primitiveTypes.put(Integer.TYPE, Integer.class);
        primitiveTypes.put(Long.TYPE, Long.class);

        if (parameterClass.isPrimitive()) {
            // If parameter class is not of actual type, get type from primitive map,
            // then get value of method
            return getValueOfMethod(primitiveTypes.get(parameterClass), parameterClass.getName());
        } else {
            return getValueOfMethod(parameterClass, parameterClass.getName());
        }
    }

    /**
     * Get the value of method for given class type for converting query params
     * from string type to requested type of resource method
     * @param queryParameter Required type for query parameter
     * @param queryParameterName Name of the query parameter
     * @return Value of method for respective type class
     */
    private static Method getValueOfMethod(final Class<?> queryParameter, final String queryParameterName) {
        try {
            return queryParameter.getDeclaredMethod("valueOf", String.class);
        } catch (NoSuchMethodException ex) {
            String error = "Error occurred while trying to get the valueOf method for query param class"
                    + queryParameterName + ". Cause: " + ex.getCause();
            throw new NoSuchMethodError(error);
        }

    }
}
