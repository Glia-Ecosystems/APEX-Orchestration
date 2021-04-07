package com.netflix.conductor.contribs.kafka.resource.handlers;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.google.inject.Inject;
import com.google.inject.ProvisionException;
import com.netflix.conductor.contribs.kafka.model.ResponsePayload;
import com.netflix.conductor.contribs.kafka.resource.builder.Resource;
import com.netflix.conductor.contribs.kafka.resource.builder.ResourceBuilder;
import com.google.inject.Injector;
import com.netflix.conductor.contribs.kafka.resource.builder.ResourceMethod;
import com.netflix.conductor.contribs.kafka.resource.builder.ResourceMethod.MethodParameter;
import com.netflix.conductor.contribs.kafka.model.RequestPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Path;
import javax.ws.rs.core.Response.Status;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.*;
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
    private final ObjectMapper objectMapper;

    @Inject
    public ResourceHandler(final Injector injector, final ObjectMapper objectMapper) {
        this.resourcesLoader = new ResourcesLoader(RESOURCE_PATH);
        this.objectMapper = objectMapper;
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
            // .: Period - Match any character, except line break
            // *: Asterisk - Match previous character zero or more times
            // ?: Question mark - Match previous zero or more times
            resourceMap.put(uri.value() + "(/.*)?", resource);
        }
    }

    /**
     * Main function for processing client requests  to the  Conductor API
     *
     * @param request Contains all the needed information for processing the request
     * @return Response from resource
     */
    public ResponsePayload processRequest(RequestPayload request) {
        // Create a response container
        final ResponsePayload response = new ResponsePayload(request);

        // Get the requested resource from the resource map
        final Resource requestedResource = getRequestedResource(request);
        // If the resource can't be found send back a response that the resource can not be found for given URI
        if (requestedResource == null) {
            response.setStatus(404);
            response.setResponseEntity(Status.NOT_FOUND);
            response.setResponseErrorMessage("Resource for requested URI '" + request.getResourceURI() + "' can not be found");
            return response;
        }
        // Remove the base URI and to get only the URI for the requested method/service of the resource
        final String methodUri = request.getResourceURI().replace(requestedResource.getPathURI().value(), "");
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
        // Set the indicator for if a start a workflow was requested
        response.setStartedAWorkflow(requestedService.getMethod().getName().equals("startWorkflow"));
        return executeRequest(request, response,requestedResource, requestedService);
    }

    /**
     * Searches the resource map for the requested resource class
     *
     * @param request Contains all the needed information for processing the request
     * @return Resource object of resource class. If not found, null is returned
     */
    private Resource getRequestedResource(final RequestPayload request) {
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
     * @param requestPayload Contains all the needed information for processing the request
     * @param resource         Resource object of the requested resource class
     * @param methodURI        URI of the requested service
     * @return ResourceMethod object of the requested method. If not found, null is returned
     */
    private ResourceMethod getRequestedService(final RequestPayload requestPayload, final Resource resource,
                                               final String methodURI) {
        for (final ResourceMethod resourceMethod : resource.getMethods().get(requestPayload.getHttpMethod())) {
            final Pattern p = Pattern.compile(resourceMethod.getUriPattern());
            final Matcher m = p.matcher(methodURI);
            if (m.matches()) {
                return resourceMethod;
            }
        }
        return null;
    }

    /**
     * Executes clients on Conductor  API
     * @param request Contains all the needed information for processing the request
     * @param response Response object for sending all needed information about the response from the Conductor API
     * @param requestedResource  Resource object of the requested resource class
     * @param requestedMethod  ResourceMethod object of the requested method
     * @return Response from resource
     */
    private ResponsePayload executeRequest(final RequestPayload request, final ResponsePayload response,
                                           final Resource requestedResource, final ResourceMethod requestedMethod) {
        Object serviceResponse = null;
        try {
            final Object resourceInstance = getResourceInstance(requestedResource.getResourceClass());
            final Object[] methodArguments = getMethodArguments(request, requestedMethod, requestedResource);
            serviceResponse = callMethod(resourceInstance, requestedMethod.getMethod(), methodArguments);
        } catch (final IllegalAccessException | IOException | IllegalArgumentException ex) {
            String logError = "Error occurred while executing the request on the resource method. Error: " + ex + " Cause: " + ex.getCause();
            String clientResponseError = "Error occurred while executing the request on the resource method. Cause: " + ex.getCause();
            logger.error(logError);
            response.setResponseErrorMessage(clientResponseError);
        } catch (final InvocationTargetException ex) {
            String logError = "Error occurred while executing the request on the resource method. Error: " + ex + " Cause: " + ex.getCause();
            String clientResponseError = "Error occurred while executing the request on the resource method. Cause: " + ex.getCause();
            logger.error(logError);
            response.setResponseErrorMessage(clientResponseError);
            return processResponse(response, null, true);
        }
        return processResponse(response, serviceResponse, false);
    }

    /**
     * Update response container with response from the Conductor API
     *
     * @param responsePayload Response object for sending all needed information about the response from the Conductor API
     * @param response          Response from the Conductor API
     * @return Contains all the needed information about the response from the Conductor API
     */
    private ResponsePayload processResponse(final ResponsePayload responsePayload, final Object response,
                                            final boolean invocationException) {
        if ("".equals(responsePayload.getResponseErrorMessage())) {
            // If no error message respond that everything went okay with process request
            responsePayload.setStatus(200);
            responsePayload.setStatusType(Status.OK);
        } else if (invocationException) {
            // If an Invocation exception occurred, set internal server error status
            responsePayload.setStatus(400);
            responsePayload.setStatusType(Status.BAD_REQUEST);
        } else {
            // If any other exception occurred, set internal server error status
            responsePayload.setStatus(500);
            responsePayload.setStatusType(Status.INTERNAL_SERVER_ERROR);
        }
        responsePayload.setResponseEntity(response);
        return responsePayload;
    }

    /**
     * Filter through the parameters of the requested method and gets the necessary arguments for the method
     *
     * @param requestPayload  Response object for sending all needed information about the response from the Conductor API
     * @param resourceMethod    ResourceMethod object of the requested method
     * @param requestedResource Resource object of the requested resource class
     * @return List of the arguments for the requested method
     */
    private Object[] getMethodArguments(final RequestPayload requestPayload, final ResourceMethod resourceMethod,
                                        final Resource requestedResource) throws IOException, InvocationTargetException, IllegalAccessException {
        final List<MethodParameter> parameters = resourceMethod.getParameters();
        final Object[] arguments = new Object[parameters.size()];
        for (int i = 0; i < parameters.size(); i++) {
            final String parameterAnnotationType = parameters.get(i).getParameterAnnotationType().name();
            // Get arguments for path param annotated parameters
            if (parameterAnnotationType.equals("PATH")) {
                arguments[i] = getPathParamValue(requestPayload.getResourceURI(), resourceMethod.getUri().value(),
                        parameters.get(i).getParameterName(), requestedResource.getPathURI().value());
                // Get arguments for query param annotated parameters
            } else if (parameterAnnotationType.equals("QUERY")) {
                arguments[i] = getQueryParamValue(requestPayload.getResourceURI(), parameters.get(i));
            } else {
                // Get arguments for entity parameters
                arguments[i] = getEntityParamValue(parameters.get(i).getParameterType(), requestPayload.getEntity());
            }
        }
        return arguments;
    }

    /**
     * Get the argument for a PathParm annotated parameter by parsing the URI sent by the client
     * example:
     * Requested resource: /workflow/{name}, @PathParam("name")
     * Client Request URI: /workflow/gliaecosystem
     * <p>
     * Argument retrieved for method: gliaecosystem
     *
     * @param requestedURI  Requested URI by client
     * @param methodURI     URI of the requested method
     * @param parameterName Name of the parameter
     * @param resourceURI   URI of the requested resource
     * @return The argument for a PathParm annotated parameter
     */
    private String getPathParamValue(final String requestedURI, final String methodURI, final String parameterName,
                                    final String resourceURI) {
        final String uriOfInterest = requestedURI.replace(resourceURI, "");
        final String[] uriSplit = uriOfInterest.split("[/?]");
        final String[] methodURISplit = methodURI.split("/");
        for (int i = 0; i < methodURISplit.length; i++) {
            if (methodURISplit[i].equals("{" + parameterName + "}")) {
                return uriSplit[i];
            }
        }
        return "";
    }

    /**
     * Get the argument for a Query annotated parameter by parsing the URI sent by the client
     * example:
     * Requested resource: /workflow/{name}, @QueryParam("example")
     * Client Request URI: /workflow/gliaecosystem?example=true
     *
     * Argument retrieved for method: true
     *
     * @param requestedURI Requested URI by client
     * @return The argument for a Query annotated parameter
     */
    private Object getQueryParamValue(final String requestedURI, final MethodParameter methodParameter) throws InvocationTargetException, IllegalAccessException {
        // Gets query parameter value (if given) from given uri and convert to required type
        if (requestedURI.contains("?") && requestedURI.contains(methodParameter.getParameterName())) {
            final String uriQueries = requestedURI.substring(requestedURI.lastIndexOf('?') + 1);
            for (final String query : uriQueries.split("&")) {
                final String[] parameter = query.split("=");
                if (parameter[0].equals(methodParameter.getParameterName())) {
                    return convertQueryParam(parameter[1], methodParameter);
                }
            }
        }
        // Gets default query parameter value and convert to required type
        return convertQueryParam(methodParameter.getParameterDefaultValue(), methodParameter);
    }

    /**
     * Convert query parameter key/value pair from requested URI to its required
     * type for the resource method
     * @param parameterValue Value of the given query parameter
     * @param methodParameter Method parameter object of the requested method
     * @return Query Parameter converted to required type
     * @throws InvocationTargetException Thrown to indicate an exception that occurred while method was being executed
     * @throws IllegalAccessException    Thrown to indicate that this method is not allowed to invoke the  method
     */
    private Object convertQueryParam(String parameterValue, final MethodParameter methodParameter) throws InvocationTargetException, IllegalAccessException {
        final Class<?> parameterClass = methodParameter.getParameterClass();
        // Null is returned as an argument when a query parameter value is not provided in request
        // and the resource do not have a default value for parameter
        if (parameterValue == null) {
            return null;
        }else if (parameterClass == List.class) {
            // Removes square brackets from list type query params, if exist
            parameterValue = parameterValue.replaceAll("[\\[\\]]", "");
            return Arrays.asList(parameterValue.split(","));
        } else if (parameterClass == String.class) {
            return parameterValue;
        } else {
            return callMethod(null, methodParameter.getQueryParamValueConverter(), parameterValue);
        }
    }

    /**
     * Gets the argument for a entity parameter
     *
     * @param parameterType Type of the parameter argument
     * @param entity        Client request of type object that will be converted to the type required by requested method
     * @return Requested method entity of required type.
     * @throws IOException Exception for if the entity fails to be turned into an Input Stream or an error occurs
     *                     converting the object type entity to the requested method required entity type
     */
    private Object getEntityParamValue(final Type parameterType, final Object entity) throws IOException {
        // Gets the object reader with required parsing configs from the object mapper provided from
        // the JsonMapperProvider class
        ObjectReader reader = objectMapper.reader();
        // Sets the parameter required type to the reader for converting the object to type
        reader = reader.forType(reader.getTypeFactory().constructType(parameterType));
        // Implements a try-with resource statement that will automatically close resources upon statement
        // complete or if abrupt as a result of an error
        try (final BufferedInputStream inputStream = new BufferedInputStream(
                new ByteArrayInputStream(objectMapper.writeValueAsString(entity).getBytes(StandardCharsets.UTF_8)))) {
            return reader.readValue(inputStream);
        }
    }

    /**
     * Docs: https://google.github.io/guice/api-docs/latest/javadoc/index.html?com/google/inject/Injector.html
     * Warns..
     * Returns the appropriate instance for the given injection key; equivalent to getProvider(key).get().
     * When feasible, avoid using this method, in favor of having Guice inject your dependencies ahead of time.
     * <p>
     * In this case the classes are dynamically created upon request of service from client and destroyed after.
     * <p>
     * Uses Injector object for dependency injection when instantiating resource classes
     *
     * @param clazz Resource class
     * @return Instance of the resource class
     */
    private Object getResourceInstance(final Class<?> clazz) {
        try {
            return injector.getInstance(clazz);
        } catch (final ProvisionException ex) {
            throw new ProvisionException("Instance of resource could not be loaded by injector", ex.getCause());
        }
    }

    /**
     * Invoke resource method
     *
     * @param resource   Resource instance
     * @param method     Requested method of the resource
     * @param parameters List of the parameters needed for the method. Parameters is a varargs which allows for many
     *                   or none number of elements to be provided
     * @return Response from the method
     * @throws InvocationTargetException Thrown to indicate an exception that occurred while method was being executed
     * @throws IllegalAccessException    Thrown to indicate that this method is not allowed to invoke the  method
     */
    private Object callMethod(final Object resource, final Method method, final Object... parameters) throws InvocationTargetException, IllegalAccessException {
        return method.invoke(resource, parameters);
    }
}
