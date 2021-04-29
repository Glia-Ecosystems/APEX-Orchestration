package com.netflix.conductor.contribs.kafka.resource.builder;

import org.springframework.web.bind.annotation.RequestMapping;

import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ResourceMethod {

    private final Method method;
    private final RequestMapping uri;
    private final String uriPattern;
    private final String httpMethod;
    private final Annotation[] annotations;
    private final Class<?> returnType;
    private final List<MethodParameter> parameters;


    public ResourceMethod(final Method method, final RequestMapping uri, final String httpMethod, final Annotation[] annotations,
                          final Class<?> returnType) {
        this.method = method;
        this.uri = uri;
        this.uriPattern = uri == null ? "" : createPatternURI(Arrays.toString(uri.value()), method);
        this.httpMethod = httpMethod;
        this.annotations = annotations;
        this.returnType = returnType;
        this.parameters = new ArrayList<>();
    }

    /**
     * Get the method object
     *
     * @return Method
     */
    public Method getMethod() {
        return method;
    }

    /**
     * Get a list of the method parameters object, containing all the necessary information
     * for each parameter in their respective object
     * @return Parameters list
     */
    public List<MethodParameter> getParameters() {
        return parameters;
    }

    /**
     * Get the URI of the method
     * @return URI
     */
    public String getUriPattern() {
        return uriPattern;
    }

    /**
     * Get the URI of the method
     *
     * @return Method URI
     */
    public RequestMapping getUri() {
        return uri;
    }

    /**
     * Creates a URI regex for pattern matching
     * This function is mainly used to created regex of braces in URI's
     * ex: Raw URI -> /workflow/{name}, Regex URI -> /workflow/([^/]+?)
     *
     * @param uri Raw URI of the method
     * @param method Method object of the resource
     * @return URI regex
     */
    private String createPatternURI(final String uri, final Method method) {
        // Regex expression: ([^/]+?)
        // (): Capturing group - parenthesis means, capture text grouped together within parenthesis and extract substring
        // []: Bracket - Match any of the characters inside the bracket
        // ^: Anchor - Means start of string, but inside of a bracket it means match any character not in set
        // /: Backslash - Look for backslash symbol
        // \\: Escape character, matches any symbol following backslash
        // +: Plus - Match one or more of the previous tokens
        // ?: Question mark - Match previous zero or more times
        // .: Period - Match any character, except line break
        final String regexForPathParams = "([^/]+?)";
        final String regexForQueryParams = "(\\?.*)?";
        // This  indicator is used to assist in parsing/building a raw URI to a regex URI
        boolean okayToProcess = true;
        final StringBuilder patternURI = new StringBuilder();
        for (int i  = 0; i < uri.length(); i++){
            final char c = uri.charAt(i);
            if (c == '{') {
                patternURI.append(regexForPathParams);
                okayToProcess = false;
            } else if  (c == '}') {
                okayToProcess = true;
            }
            if (okayToProcess && c != '}'){
                patternURI.append(c);
            }
        }
        // If regexPattern not added for PathParam, then add regexPattern at the end of
        // URI for QueryParam, for if a query param is given to URI
        return checkIfAnyQueryParamExist(method) ? patternURI.toString() + regexForQueryParams: patternURI.toString();
    }

    /**
     * Checks if the method have any query parameters annotations
     *
     * @param method Method object of the resource
     * @return Indicator if method have any query parameters
     */
    private boolean checkIfAnyQueryParamExist(final Method method){
        final Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        for (Annotation[] annotationList: parameterAnnotations){
            for (Annotation annotation: annotationList){
                if (QueryParam.class == annotation.annotationType()){
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * MethodParameter object of the method parameter
     * Contains all the information needed of a parameter of a method
     *
     */
    public static class MethodParameter {

        public enum ParameterAnnotationType {ENTITY, QUERY, PATH}

        private final Class<?> parameterClass;
        private final Type parameterType;
        private final Annotation parameterAnnotation;
        private final ParameterAnnotationType parameterAnnotationType;
        private final String parameterDefaultValue;
        private final String parameterName;
        private final Method queryParamValueConverter;

        public MethodParameter(final Class<?> parameterClass, final Type parameterType, final Annotation parameterAnnotation,
                               final ParameterAnnotationType parameterAnnotationType, final String parameterDefaultValue,
                               final String parameterName, final Method queryParamValueConverter) {
            this.parameterClass = parameterClass;
            this.parameterType = parameterType;
            this.parameterAnnotation = parameterAnnotation;
            this.parameterAnnotationType = parameterAnnotationType;
            this.parameterDefaultValue = parameterDefaultValue;
            this.parameterName = parameterName;
            this.queryParamValueConverter = queryParamValueConverter;
        }

        /**
         * Get the annotation type of the parameter
         * @return Parameter annotation type
         */
        public ParameterAnnotationType getParameterAnnotationType() {
            return parameterAnnotationType;
        }

        /**
         * Get the default value if specified for the parameter
         * @return Default value
         */
        public String getParameterDefaultValue() {
            return parameterDefaultValue;
        }

        /**
         * Get the name of the parameter
         *
         * @return Parameter name
         */
        public String getParameterName() {
            return parameterName;
        }

        /**
         * Get the type object of the parameter
         *
         * @return Parameter type
         */
        public Type getParameterType() {
            return parameterType;
        }

        /**
         * Get the class object of the parameter
         *
         * @return Parameter class
         */
        public Class<?> getParameterClass() {
            return parameterClass;
        }

        /**
         * Get the value of method (if provided) for converting the query parameter
         * from string to required type
         *
         * @return Type.valueOfMethod
         */
        public Method getQueryParamValueConverter() {
            return queryParamValueConverter;
        }
    }
}
