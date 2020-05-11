package com.netflix.conductor.contribs.kafka.resource.builder;

import javax.ws.rs.Path;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class ResourceMethod {

    private final Method method;
    private final Path uri;
    private final String uriPattern;
    private final String httpMethod;
    private final Annotation[] annotations;
    private final Class returnType;
    private final List<MethodParameter> parameters;
    public enum Source {ENTITY, QUERY, MATRIX, PATH, COOKIE, HEADER, CONTEXT, FORM, UNKNOWN};


    public ResourceMethod(Method method, Path uri, String httpMethod, Annotation[] annotations, Class returnType) {
        this.method = method;
        this.uri = uri;
        this.uriPattern = uri == null ? "" : createUriPattern(uri.value());
        this.httpMethod = httpMethod;
        this.annotations = annotations;
        this.returnType = returnType;
        this.parameters =  new ArrayList<MethodParameter>();
    }

    public Method getMethod() {
        return method;
    }

    public List<MethodParameter> getParameters() {
        return parameters;
    }

    public String getUriPattern() {
        return uriPattern;
    }

    private String createUriPattern(String uri){
        String regexForBraces = "([^/]+?)";
        boolean okayToProcess = true;
        StringBuilder uriPattern = new StringBuilder();
        for (int i  = 0; i < uri.length(); i++){
            char c = uri.charAt(i);
            if (c == '{') {
                uriPattern.append(regexForBraces);
                okayToProcess = false;
            } else if  (c == '}') {
                okayToProcess = true;
            }
            if (okayToProcess && c != '}'){
                uriPattern.append(c);
            }
        }
        return uriPattern.toString();
    }

    public static class MethodParameter {

        private final Class<?> parameterClass;
        private final Type parameterType;
        private final Annotation parameterAnnotation;
        private final Source parameterSource ;
        private final String parameterName;
        private final String parameterDefault;

        public MethodParameter(Class<?> parameterClass, Type parameterType, Annotation parameterAnnotation,
                               Source parameterSource, String parameterName, String parameterDefault){
            this.parameterClass = parameterClass;
            this.parameterType = parameterType;
            this.parameterAnnotation = parameterAnnotation;
            this.parameterSource = parameterSource;
            this.parameterName = parameterName;
            this.parameterDefault = parameterDefault;
        }
    }
}
