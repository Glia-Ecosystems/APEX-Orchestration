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

    public Long getHeartbeatTimeStampMS() {
        return heartbeatTimeStampMS;
    }

    /**
     *
     * @return
     */
    public boolean isDeserializationErrorOccurred() {
        return deserializationErrorOccurred;
    }

    /**
     *
     * @param deserializationErrorOccurred
     */
    public void setDeserializationErrorOccurred(boolean deserializationErrorOccurred) {
        this.deserializationErrorOccurred = deserializationErrorOccurred;
    }

    /**
     *
     * @return
     */
    public String getDeserializationError() {
        return deserializationError;
    }

    /**
     *
     * @param deserializationError
     */
    public void setDeserializationError(String deserializationError) {
        this.deserializationError = deserializationError;
    }
}
