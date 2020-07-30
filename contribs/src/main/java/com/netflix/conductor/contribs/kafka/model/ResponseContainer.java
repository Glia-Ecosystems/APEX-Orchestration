package com.netflix.conductor.contribs.kafka.model;

import javax.ws.rs.core.Response;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

public class ResponseContainer {

    private final String dateTime;
    private final Map<String, Object> request;
    private int status;
    private Response.StatusType statusType;
    private String responseErrorMessage;
    private Object responseEntity;
    private boolean startedAWorkflow;

    public ResponseContainer(final RequestContainer request) {
        this.dateTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("E, MMM dd yyyy HH:mm:ss"));
        this.request = request.getRequestData();
        this.status = 204;
        this.statusType = Response.Status.NO_CONTENT;
        this.responseEntity = "";
        this.responseErrorMessage = "";
        this.startedAWorkflow = false;
    }

    /**
     * This constructor is used for returning a ResponseContainer object when an error occurred before the
     * client request was sent to the service
     */
    public ResponseContainer() {
        this.dateTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("E, MMM dd yyyy HH:mm:ss"));
        this.request = null;
        this.status = 204;
        this.statusType = Response.Status.NO_CONTENT;
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
    public void setStatusType(final Response.StatusType statusType) {
        this.statusType = statusType;
    }

    /**
     * Set the status code of the response
     *
     * @param status Status code
     */
    public void setStatus(final int status) {
        this.status = status;
    }

    /**
     * Get the status code of the response
     *
     * @return response current status code
     */
    public int getStatus() {
        return status;
    }

    public String getResponseErrorMessage() {
        return responseErrorMessage;
    }

    /**
     * Set the indication of if a workflow was started during the request
     *
     * @param startedAWorkflow Indicator of if a workflow was started
     */
    public void setStartedAWorkflow(boolean startedAWorkflow) {
        this.startedAWorkflow = startedAWorkflow;
    }

    /**
     * Get the indication of if a workflow was started during the request
     *
     * @return Indicator of if a workflow was started
     */
    public boolean isStartedAWorkflow() {
        return startedAWorkflow;
    }

    /**
     * Get the entity of the response from Conductor
     *
     * @return response entity
     */
    public Object getResponseEntity() {
        return responseEntity;
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

    /**
     * Creates a to String object for printing the responseContainer object
     * @return String object of the class
     */
    @Override
    public String toString() {
        return "ResponseContainer{" +
                "dateTime='" + dateTime + '\'' +
                ", request=" + request +
                ", status=" + status +
                ", statusType=" + statusType +
                ", responseErrorMessage='" + responseErrorMessage + '\'' +
                ", responseEntity=" + responseEntity +
                ", startedAWorkflow=" + startedAWorkflow +
                '}';
    }
}
