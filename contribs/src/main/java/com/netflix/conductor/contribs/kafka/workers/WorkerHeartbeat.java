package com.netflix.conductor.contribs.kafka.workers;

public class WorkerHeartbeat {

    private final Long heartbeatTimeStampMS;
    // The below fields are used for exception handling in kafka streams during deserialization
    private boolean deserializationErrorOccurred;
    private String deserializationError;

    public WorkerHeartbeat(final long heartbeatMS) {
        this.heartbeatTimeStampMS = heartbeatMS;
        this.deserializationErrorOccurred = false;
        this.deserializationError = "";
    }

    /**
     *  Get the heartbeat timestamp
     *
     * @return Heartbeat TimeStamp MS
     */
    public Long getHeartbeatTimeStampMS() {
        return heartbeatTimeStampMS;
    }

    /**
     * Get indication of if an error occurred during deserialization of request
     *
     * @return Indicator if an error occurred or not
     */
    public boolean isDeserializationErrorOccurred() {
        return deserializationErrorOccurred;
    }

    /**
     * Set indication of if an error occurred during deserialization of request
     *
     * @param deserializationErrorOccurred Indicator if an error occurred
     */
    public void setDeserializationErrorOccurred(boolean deserializationErrorOccurred) {
        this.deserializationErrorOccurred = deserializationErrorOccurred;
    }

    /**
     * Get error message that occurred during deserialization of request
     *
     * @return Error message of error that occurred during deserialization of request
     */
    public String getDeserializationError() {
        return deserializationError;
    }

    /**
     * Set error message that occurred during deserialization of request
     *
     * @param deserializationError Error message of error that occurred during deserialization of request
     */
    public void setDeserializationError(String deserializationError) {
        this.deserializationError = deserializationError;
    }
}
