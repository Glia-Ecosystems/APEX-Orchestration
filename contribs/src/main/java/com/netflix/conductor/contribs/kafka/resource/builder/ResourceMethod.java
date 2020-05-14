package com.netflix.conductor.contribs.kafka.resource.builder;

import javax.ws.rs.Path;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * ResourceMethod object of resource method
 * Contains all the information needed for use of the method of a resource
 *
 * @author Glia Ecosystems
 */
public class ResourceMethod {

    private final Method method;
    private final Path uri;
    private final String uriPattern;
    private final String httpMethod;
    private final Annotation[] annotations;
    private final Class<?> returnType;
    private final List<MethodParameter> parameters;


    public ResourceMethod(final Method method, final Path uri, final String httpMethod, final Annotation[] annotations,
                          final Class<?> returnType) {
        this.method = method;
        this.uri = uri;
        this.uriPattern = uri == null ? "" : createPatternURI(uri.value());
        this.httpMethod = httpMethod;
        this.annotations = annotations;
        this.returnType = returnType;
        this.parameters = new ArrayList<>();
    }

    /**
     * Get the method object
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
     * @return Method URI
     */
    public Path getUri() {
        return uri;
    }

    /**
     * Creates a URI regex for pattern matching
     * This function is mainly used to created regex of braces in URI's
     * ex: Raw URI -> /workflow/{name}, Regex URI -> /workflow/([^/]+?)
     *
     * @param uri Raw URI of the method
     * @return URI regex
     */
    private String createPatternURI(final String uri) {
        // Regex expression: (/.*)?
        // (): Capturing group - parenthesis means, capture text grouped together within parenthesis
        // []: Bracket - Match any of the characters inside the bracket
        // ^: Anchor - Means start of string, but inside of a bracket it means "not"
        // /: Backslash - Look for backslash symbol
        // +: Plus - One or more
        // ?: Question mark - Match previous zero or more times
        final String regexForBraces = "([^/]+?)";
        // This  indicator is used to assist in parsing/building a raw URI to a regex URI
        boolean okayToProcess = true;
        final StringBuilder patternURI = new StringBuilder();
        for (int i  = 0; i < uri.length(); i++){
            final char c = uri.charAt(i);
            if (c == '{') {
                patternURI.append(regexForBraces);
                okayToProcess = false;
            } else if  (c == '}') {
                okayToProcess = true;
            }
            if (okayToProcess && c != '}'){
                patternURI.append(c);
            }
        }
        return patternURI.toString();
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

        public MethodParameter(final Class<?> parameterClass, final Type parameterType, final Annotation parameterAnnotation,
                               final ParameterAnnotationType parameterAnnotationType, final String parameterDefaultValue,
                               final String parameterName) {
            this.parameterClass = parameterClass;
            this.parameterType = parameterType;
            this.parameterAnnotation = parameterAnnotation;
            this.parameterAnnotationType = parameterAnnotationType;
            this.parameterDefaultValue = parameterDefaultValue;
            this.parameterName = parameterName;
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

    }
}
