package com.netflix.conductor.contribs.kafka.resource.handlers;

import com.google.inject.Inject;
import com.google.inject.ProvisionException;
import com.netflix.conductor.contribs.kafka.resource.builder.Resource;
import com.netflix.conductor.contribs.kafka.resource.builder.ResourceBuilder;
import com.google.inject.Injector;
import com.netflix.conductor.contribs.kafka.resource.builder.ResourceMethod;
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
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ResourceHandler {

    private static final Logger logger = LoggerFactory.getLogger(ResourceHandler.class);
    private String RESOURCE_PATH = "com.netflix.conductor.server.resources";

    private final ResourcesLoader resourcesLoader;
    private final ResourceUtilities util;
    private final Map<String, Resource> resourceMap = new HashMap<String, Resource>();
    private final Injector injector;

    @Inject
    public ResourceHandler(Injector injector) {
        this.util = new ResourceUtilities();
        this.resourcesLoader = new ResourcesLoader(RESOURCE_PATH, util);
        this.injector = injector;
        init();
    }

    private void init() {
        // Load resources from resource package path
        resourcesLoader.locateResources();

        // Build resource map for processing client requests
        for (final Class<?> clazz: resourcesLoader.getClasses()){
            Path uri = clazz.getAnnotation(Path.class);
            Resource resource = ResourceBuilder.buildResource(new Resource(clazz, uri));
            resourceMap.put(uri.value() + "(/.*)?", resource);
        }

        //practiceProcessRequest(verifyRequestedURIPath("/metadata/taskdefs"), verifyRequestedHTTPMethod("GET"), "");
    }

    public ResponseContainer processRequest(final String path, final String httpMethod, final Object entity) {
        RequestContainer request = new RequestContainer(path, httpMethod, entity);
        ResponseContainer response = new ResponseContainer(request);

        Resource requestedResource = getRequestedResource(request);
        if (requestedResource == null) {
            response.setStatus(404);
            response.setResponseEntity(Status.NOT_FOUND);
            response.setResponseErrorMessage("Resource for requested URI " + request.getResourceURI() + "can not be found");
            return response;
        }

        String methodUri = path.replace(requestedResource.getPathURI().value(), "");
        ResourceMethod requestedService = getRequestedService(request, requestedResource, methodUri);
        if (requestedService == null){
            response.setStatus(404);
            response.setResponseEntity(Status.NOT_FOUND);
            response.setResponseErrorMessage("Requested service of requested resource can not be found by given URI: "
            + request.getResourceURI());
            return response;
        }
        return __executeRequest(request, response,requestedResource, requestedService);
    }


    private Resource getRequestedResource(RequestContainer request){
        for (String k: resourceMap.keySet()) {
            Pattern p = Pattern.compile(k);
            Matcher m = p.matcher(request.getResourceURI());
            if(m.matches()){
                return resourceMap.get(k);
            }
        }
        return null;
    }

    private ResourceMethod getRequestedService(RequestContainer requestContainer, Resource resource, String methodURI){
        for (ResourceMethod resourceMethod: resource.getMethods().get(requestContainer.getHttpMethod())){
            Pattern p = Pattern.compile(resourceMethod.getUriPattern());
            Matcher m = p.matcher(methodURI);
            if(m.matches()){
                return resourceMethod;
            }
        }
        return null;
    }

    public String verifyRequestedURIPath(String path){
        return path.startsWith("/") ? path : "/" + path;
    }

    public String verifyRequestedHTTPMethod(String httpMethod){
        return httpMethod.toUpperCase();
    }


    private ResponseContainer __executeRequest(final RequestContainer request, final ResponseContainer response, final Resource requestedResource,
                               final ResourceMethod requestedMethod) {
        Object serviceResponse = null;
        try {
            Object resourceInstance = __getResourceInstance(requestedResource.getResource());
            if (requestedMethod.getParameters().size() != 0) {
                serviceResponse = __callService(resourceInstance, requestedMethod.getMethod(), request.getEntity());
            } else {
                serviceResponse = __callService(resourceInstance, requestedMethod.getMethod());
            }
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return processResponse(response, serviceResponse);
    }

    private ResponseContainer processResponse(ResponseContainer responseContainer, Object response){
        if (response != null){
            responseContainer.setStatus(200);
            responseContainer.setResponseEntity(Status.OK);
        }
        return responseContainer;
    }

    /**
     *
     * Docs: https://google.github.io/guice/api-docs/latest/javadoc/index.html?com/google/inject/Injector.html
     * Warns..
     * Returns the appropriate instance for the given injection key; equivalent to getProvider(key).get().
     * When feasible, avoid using this method, in favor of having Guice inject your dependencies ahead of time.
     *
     * In this case the classes are dynamically created upon request of service from client and destroyed after.
     */
    private Object __getResourceInstance(Class<?> clazz) throws Throwable {
        try {
            return injector.getInstance(clazz);
        } catch (ProvisionException ex) {
            logger.error(String.valueOf(ex.getErrorMessages()));
            throw ex.getCause();
        }
    }

    private Object __callService(Object resource, Method method, Object... parameters) throws InvocationTargetException, IllegalAccessException {
        return method.invoke(resource, parameters);
    }

    private Object __callService(Object resource, Method method) throws InvocationTargetException, IllegalAccessException {
        return method.invoke(resource);
    }

    private static class RequestContainer{

        private final String resourceURI;
        private final String httpMethod;
        private final Object entity;

        public RequestContainer(final String resourceURI, final String httpMethod, final Object entity){
            this.resourceURI = resourceURI;
            this.httpMethod = httpMethod;
            this.entity = entity;
        }

        public String getResourceURI() {
            return resourceURI;
        }

        public String getHttpMethod() {
            return httpMethod;
        }

        public Object getEntity() {
            return entity;
        }
    }

    public static class ResponseContainer{

        private final String dateTime;
        private final RequestContainer request;
        private int status;
        private StatusType statusType;
        private String responseErrorMessage;
        private Object responseEntity;

        public ResponseContainer(RequestContainer request) {
            this.dateTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("E, MMM dd yyyy HH:mm:ss"));
            this.request = request;
            this.status = 204;
            this.statusType = Status.NO_CONTENT;
            this.responseEntity = null;
            this.responseErrorMessage = "";

        }

        public ResponseContainer() {
            this.dateTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("E, MMM dd yyyy HH:mm:ss"));
            this.request = null;
            this.status = 204;
            this.statusType = Status.NO_CONTENT;
            this.responseEntity = null;
            this.responseErrorMessage = "";

        }

        public void setResponseEntity(Object responseEntity) {
            this.responseEntity = responseEntity;
        }

        public void setResponseErrorMessage(String responseErrorMessage) {
            this.responseErrorMessage = responseErrorMessage;
        }
        public void setStatusType(StatusType statusType) {
            this.statusType = statusType;
        }
        public void setStatus(int status) {
            this.status = status;
        }

        public String getDateTime() {
            return dateTime;
        }

        public int getStatus() {
            return status;
        }

        public String getResponseErrorMessage() {
            return responseErrorMessage;
        }

        public StatusType getStatusType() {
            return statusType;
        }

        public Object getResponseEntity() {
            return responseEntity;
        }
    }
}
