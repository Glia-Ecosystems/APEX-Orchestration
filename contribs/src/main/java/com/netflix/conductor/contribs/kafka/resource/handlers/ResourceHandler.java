package com.netflix.conductor.contribs.kafka.resource.handlers;

import com.google.inject.Inject;
import com.google.inject.ProvisionException;
import com.netflix.conductor.contribs.kafka.resource.builder.Resource;
import com.netflix.conductor.contribs.kafka.resource.builder.ResourceBuilder;
import com.google.inject.Injector;
import com.netflix.conductor.contribs.kafka.resource.builder.ResourceMethod;
import com.netflix.conductor.contribs.kafka.resource.builder.ResourceMethod.MethodParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Path;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.Response.StatusType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Handles the resources for processing client requests to the Conductor API
 *
 * @author Glia Ecosystems
 */
public class ResourceHandler {

    private static final Logger logger = LoggerFactory.getLogger(ResourceHandler.class);
    private static final String RESOURCE_PATH = "com.netflix.conductor.server.resources";
    private final ResourcesLoader resourcesLoader;
    private final Map<String, Resource> resourceMap = new HashMap<>();
    private final Injector injector;

    @Inject
    public ResourceHandler(final Injector injector) {
        this.resourcesLoader = new ResourcesLoader(RESOURCE_PATH);
        this.injector = injector;
        init();
    }

    /**
     * Loads the needed resources and place them in a map object for dynamically retrieving needed resources upon request
     */
    private void init() {
        // Load resources from resource package path
        resourcesLoader.locateResources();

        // Build resource map for processing client requests
        for (final Class<?> clazz : resourcesLoader.getClasses()) {
            final Path uri = clazz.getAnnotation(Path.class);
            final Resource resource = ResourceBuilder.buildResource(new Resource(clazz, uri));
            // Regex expression: (/.*)?
            // (): Capturing group - parenthesis means, capture text grouped together within parenthesis
            // /: Backslash - Look for backslash symbol
            // .: Period - Match any character
            // *: Asterisk - Match previous character zero or more times
            // ?: Question mark - Match previous zero or more times
            resourceMap.put(uri.value() + "(/.*)?", resource);
        }
    }

    /**
     * Main function for processing client requests  to the  Conductor API
     * @param path URI path of the requested resource
     * @param httpMethod HTTP method to assist in identify the requested service from the resource
     * @param entity The argument to be sent to the resource service
     * @return Response from resource
     */
    public ResponseContainer processRequest(final String path, final String httpMethod, final Object entity) {
        // Create a request and response container
        final RequestContainer request = new RequestContainer(path, httpMethod, entity);
        final ResponseContainer response = new ResponseContainer(request);

        // Get the requested resource from the resource map
        final Resource requestedResource = getRequestedResource(request);
        // If the resource can't be found send back a response that the resource can not be found for given URI
        if (requestedResource == null) {
            response.setStatus(404);
            response.setResponseEntity(Status.NOT_FOUND);
            response.setResponseErrorMessage("Resource for requested URI " + request.getResourceURI() + " can not be found");
            return response;
        }
        // Remove the base URI and to get only the URI for the requested method/service of the resource
        final String methodUri = path.replace(requestedResource.getPathURI().value(), "");
        // Get the requested method from the resource
        final ResourceMethod requestedService = getRequestedService(request, requestedResource, methodUri);
        // If the method can't be found send back a response that the method can not be found for given URI
        if (requestedService == null){
            response.setStatus(404);
            response.setResponseEntity(Status.NOT_FOUND);
            response.setResponseErrorMessage("Requested service of requested resource can not be found by given URI: "
            + request.getResourceURI());
            return response;
        }
        return executeRequest(request, response,requestedResource, requestedService);
    }

    /**
     * Searches the resource map for the requested resource class
     *
     * @param request Contains all the needed information for processing the request
     * @return Resource object of resource class. If not found, null is returned
     */
    private Resource getRequestedResource(final RequestContainer request) {
        for (final Map.Entry<String, Resource> entry : resourceMap.entrySet()) {
            final Pattern p = Pattern.compile(entry.getKey());
            final Matcher m = p.matcher(request.getResourceURI());
            if (m.matches()) {
                return entry.getValue();
            }
        }
        return null;
    }

    /**
     * Searches the resource method map for the requested service
     *
     * @param requestContainer Contains all the needed information for processing the request
     * @param resource         Resource object of the requested resource class
     * @param methodURI        URI of the requested service
     * @return ResourceMethod object of the requested method. If not found, null is returned
     */
    private ResourceMethod getRequestedService(final RequestContainer requestContainer, final Resource resource,
                                               final String methodURI) {
        for (final ResourceMethod resourceMethod : resource.getMethods().get(requestContainer.getHttpMethod())) {
            final Pattern p = Pattern.compile(resourceMethod.getUriPattern());
            final Matcher m = p.matcher(methodURI);
            if (m.matches()) {
                return resourceMethod;
            }
        }
        return null;
    }

    /**
     * Verifies the given URI is of correct syntax
     *
     * @param path Given URI from client request
     * @return URI for requested resource
     */
    public String verifyRequestedURIPath(final String path) {
        return path.startsWith("/") ? path : "/" + path;
    }

    /**
     * Verifies the given HTTP method is capitalized
     *
     * @param httpMethod Given HTTP method from client request
     * @return Upper Case HTTP method
     */
    public String verifyRequestedHTTPMethod(final String httpMethod) {
        return httpMethod.toUpperCase();
    }

    /**
     * Executes clients on Conductor  API
     * @param request Contains all the needed information for processing the request
     * @param response Response object for sending all needed information about the response from the Conductor API
     * @param requestedResource  Resource object of the requested resource class
     * @param requestedMethod  ResourceMethod object of the requested method
     * @return Response from resource
     */
    private ResponseContainer executeRequest(final RequestContainer request, final ResponseContainer response,
                                             final Resource requestedResource, final ResourceMethod requestedMethod) {
        Object serviceResponse = null;
        try {
            final Object resourceInstance = getResourceInstance(requestedResource.getResourceClass());
            final Object[] methodArguments = getMethodArguments(request, requestedMethod, requestedResource);
            serviceResponse = callService(resourceInstance, requestedMethod.getMethod(), methodArguments);
        } catch (final Exception ex) {
            logger.error("Error occurred while executing the request on the resource method", ex);
            response.setResponseErrorMessage("Error occurred while executing the request on the resource method. " +
                    "Error: " + ex);
        }
        return processResponse(response, serviceResponse);
    }

    /**
     * Update response container with response from the Conductor API
     * @param responseContainer  Response object for sending all needed information about the response from the Conductor API
     * @param response Response from the Conductor API
     * @return Contains all the needed information about the response from the Conductor API
     */
    private ResponseContainer processResponse(final ResponseContainer responseContainer, final Object response) {
        if ("".equals(responseContainer.responseErrorMessage)) {
            responseContainer.setStatus(500);
            responseContainer.setStatusType(Status.INTERNAL_SERVER_ERROR);
        } else {
            responseContainer.setStatus(200);
            responseContainer.setStatusType(Status.OK);
        }
        responseContainer.setResponseEntity(response);
        return responseContainer;
    }

    /**
     * Filter through the parameters of the requested method and gets the necessary arguments for the method
     *
     * @param requestContainer  Response object for sending all needed information about the response from the Conductor API
     * @param resourceMethod    ResourceMethod object of the requested method
     * @param requestedResource Resource object of the requested resource class
     * @return List of the arguments for the requested method
     */
    private Object[] getMethodArguments(final RequestContainer requestContainer, final ResourceMethod resourceMethod,
                                        final Resource requestedResource) {
        final List<MethodParameter> parameters = resourceMethod.getParameters();
        final Object[] arguments = new Object[parameters.size()];
        for (int i = 0; i < parameters.size(); i++) {
            final String parameterAnnotationType = parameters.get(i).getParameterAnnotationType().name();
            if (parameterAnnotationType.equals("PATH")) {
                arguments[i] = getPathParmValue(requestContainer.getResourceURI(), resourceMethod.getUri().value(),
                        parameters.get(i).getParameterName(), requestedResource.getPathURI().value());
            } else if (parameterAnnotationType.equals("QUERY")) {
                arguments[i] = getQueryParmValue(requestContainer.getResourceURI(), parameters.get(i).getParameterName(),
                        parameters.get(i).getParameterDefaultValue());
            } else {
                arguments[i] = requestContainer.getEntity();
            }
        }
        return arguments;
    }

    /**
     * Get the argument for a PathParm annotated parameter
     *
     * @param requestedURI  Requested URI by client
     * @param methodURI     URI of the requested method
     * @param parameterName Name of the parameter
     * @param resourceURI   URI of the requested resource
     * @return The argument for a PathParm annotated parameter
     */
    private String getPathParmValue(final String requestedURI, final String methodURI, final String parameterName,
                                    final String resourceURI) {
        final String uriOfInterest = requestedURI.replace(resourceURI, "");
        final String[] uriSplit = uriOfInterest.split("/");
        final String[] methodURISplit = methodURI.split("/");
        for (int i = 0; i < methodURISplit.length; i++) {
            if (methodURISplit[i].equals("{" + parameterName + "}")) {
                return uriSplit[i];
            }
        }
        return "";
    }

    /**
     * Get the argument for a Query annotated parameter
     * @param requestedURI Requested URI by client
     * @param parameterName Name of the parameter
     * @param parameterDefaultValue Default value for query parameter if not provided with client request
     * @return The argument for a Query annotated parameter
     */
    private String getQueryParmValue(final String requestedURI, final String parameterName, final String parameterDefaultValue){
        if (requestedURI.contains("?") && requestedURI.contains(parameterName)) {
            final String uriQueries = requestedURI.substring(requestedURI.lastIndexOf('?') + 1);
            for (final String query : uriQueries.split("&")) {
                final String[] parameter = query.split("=");
                if (parameter[0].equals("version")) {
                    return parameter[1];
                }
            }
        }
        return parameterDefaultValue;
    }

    /**
     *
     * Docs: https://google.github.io/guice/api-docs/latest/javadoc/index.html?com/google/inject/Injector.html
     * Warns..
     * Returns the appropriate instance for the given injection key; equivalent to getProvider(key).get().
     * When feasible, avoid using this method, in favor of having Guice inject your dependencies ahead of time.
     *
     * In this case the classes are dynamically created upon request of service from client and destroyed after.
     *
     * Uses Injector object for dependency injection when instantiating resource classes
     * @param clazz Resource class
     * @return Instance of the resource class
     */
    private Object getResourceInstance(final Class<?> clazz) {
        try {
            return injector.getInstance(clazz);
        } catch (final ProvisionException ex) {
            throw new ProvisionException("Instance of resource could not be loaded by injector", ex);
        }
    }

    /**
     * Invoke resource method
     * @param resource Resource instance
     * @param method Requested method of the resource
     * @param parameters List of the parameters needed for the method. Parameters is a varargs which allows for many
     *                   or none number of elements to be provided
     * @return Response from the method
     * @throws InvocationTargetException Thrown to indicate an exception that occurred while method was being executed
     * @throws IllegalAccessException Thrown to indicate that this method is not allowed to invoke the  method
     */
    private Object callService(final Object resource, final Method method, final Object... parameters) throws InvocationTargetException, IllegalAccessException {
        return method.invoke(resource, parameters);
    }

    /**
     * Container object for the client request
     */
    private static class RequestContainer{

        private final String resourceURI;
        private final String httpMethod;
        private final Object entity;

        public RequestContainer(final String resourceURI, final String httpMethod, final Object entity){
            this.resourceURI = resourceURI;
            this.httpMethod = httpMethod;
            this.entity = entity;
        }

        /**
         * Get the requested URI
         * @return URI
         */
        public String getResourceURI() {
            return resourceURI;
        }

        /**
         * Get the requested HTTP method
         * @return HTTP method
         */
        public String getHttpMethod() {
            return httpMethod;
        }

        /**
         * Get the entity for the request
         *
         * @return Entity
         */
        public Object getEntity() {
            return entity;
        }

        /**
         * Creates a map containing the field values of Request Container
         *
         * @return Map of the field values of the class
         */
        public Map<String, Object> getRequestData() {
            final Map<String, Object> resquestData = new HashMap<>();
            resquestData.put("resourceURI", resourceURI);
            resquestData.put("httpMethod", httpMethod);
            resquestData.put("entity", entity);
            return resquestData;
        }
    }

    /**
     * Container object for the Conductor API response
     */
    public static class ResponseContainer {

        private final String dateTime;
        private final Map<String, Object> request;
        private int status;
        private StatusType statusType;
        private String responseErrorMessage;
        private Object responseEntity;

        public ResponseContainer(final RequestContainer request) {
            this.dateTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("E, MMM dd yyyy HH:mm:ss"));
            this.request = request.getRequestData();
            this.status = 204;
            this.statusType = Status.NO_CONTENT;
            this.responseEntity = "";
            this.responseErrorMessage = "";
        }

        /**
         * This constructor is used for returning a ResponseContainer object when an error occurred before the
         * client request was sent to the service
         */
        public ResponseContainer() {
            this.dateTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("E, MMM dd yyyy HH:mm:ss"));
            this.request = null;
            this.status = 204;
            this.statusType = Status.NO_CONTENT;
            this.responseEntity = "";
            this.responseErrorMessage = "";

        }

        /**
         * Set the response from the resource method to the conatiner
         *
         * @param responseEntity The response from the service
         */
        public void setResponseEntity(final Object responseEntity) {
            this.responseEntity = responseEntity;
        }

        /**
         * Set the error message if an error have occurred
         *
         * @param responseErrorMessage Error message
         */
        public void setResponseErrorMessage(final String responseErrorMessage) {
            this.responseErrorMessage = responseErrorMessage;
        }

        /**
         * Set the status type of the response from the service
         *
         * @param statusType Response status enum
         */
        public void setStatusType(final StatusType statusType) {
            this.statusType = statusType;
        }

        /**
         * Set the status code of the response
         *
         * @param status Status   code
         */
        public void setStatus(final int status) {
            this.status = status;
        }

        /**
         * Creates a map containing the field values of Response Container
         *
         * @return Map of the field values of the class
         */
        public Map<String, Object> getResponseData() {
            final Map<String, Object> responseData = new HashMap<>();
            responseData.put("dateTime", dateTime);
            responseData.put("request", (request == null) ? "" : request);
            responseData.put("status", status);
            responseData.put("statusType", statusType);
            responseData.put("responseEntity", (responseEntity == null) ? "" : responseEntity);
            responseData.put("responseErrorMessage", responseErrorMessage);
            return responseData;
        }

    }
}
