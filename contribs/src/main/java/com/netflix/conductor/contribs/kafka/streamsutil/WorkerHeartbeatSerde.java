package com.netflix.conductor.contribs.kafka.streamsutil;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.netflix.conductor.contribs.kafka.workers.WorkerHeartbeat;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.util.Map;

public class WorkerHeartbeatSerde implements Serde<WorkerHeartbeat> {

    private static final Logger logger = LoggerFactory.getLogger(WorkerHeartbeatSerde.class);
    private final Gson gson;
    private final Type longMapType;
    private String errorMessage;

    public WorkerHeartbeatSerde(){
        this.longMapType = new TypeToken<Map<String, Long>>() {}.getType();
        this.gson = new Gson();
    }

    /**
     * Configure the underlying serializer and deserializer.
     *
     * @param configs Map object containing the configurations
     * @param isKey Indicator if configurations is for key or value
     */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // This method is left empty until needed.
    }

    /**
     * Close this serde class, which will close the underlying serializer and deserializer.
     * This method has to be idempotent because it might be called multiple times.
     */
    @Override
    public void close() {
        // This method is left empty until needed.
    }

    /**
     * Creates the serializer for this class.
     * @return A Request Container Serializer
     */
    @Override
    public Serializer<WorkerHeartbeat> serializer() {
        return null;
    }

    @Override
    public Deserializer<WorkerHeartbeat> deserializer() {
        return new Deserializer<WorkerHeartbeat>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                // This method is left empty until needed.
            }

            /**
             * Handles the deserialization of a record received from Kafka to a WorkerHeartbeat object
             *
             * @param topic The topic the heartbeat was consumed from
             * @param receivedHeartbeat The heartbeat received from topic
             * @return WorkerHeartbeat object of the heartbeat received
             */
            @Override
            public WorkerHeartbeat deserialize(String topic, byte[] receivedHeartbeat) {
                errorMessage = "";
                final Map<String, Long> heartbeat = gson.fromJson(new String(receivedHeartbeat), longMapType);
                // Verify if record is null, if not verify heartbeat.
                if (heartbeat == null || receivedHeartbeatErrors(heartbeat)){
                    if (errorMessage.equals("")){
                        // Record sent is null
                        errorMessage = "Heartbeat sent to conductor is Null";
                    }
                    WorkerHeartbeat workerHeartbeat = new WorkerHeartbeat(0);
                    workerHeartbeat.setDeserializationErrorOccurred(true);
                    workerHeartbeat.setDeserializationError(errorMessage);
                    return workerHeartbeat;
                }
                return new WorkerHeartbeat(heartbeat.get("HB"));
            }

            @Override
            public void close() {

            }
        };
    }

    /**
     * Verifies the payload from the message to verify that necessary information were provided for the Conductor API
     * @param heartbeat Map object of the heartbeat sent to the Conductor via Kafka
     * @return Indicator of if the message contains all required information.
     */
    public boolean receivedHeartbeatErrors(final Map<String, Long> heartbeat){
        if (!heartbeat.containsKey("HB")){
            errorMessage = "Received heartbeat sent via kafka contain incorrect key";
            logger.error(errorMessage);
            return true;
        } else if (heartbeat.get("HB") == null) {
            errorMessage = "Received heartbeat sent via kafka contain missing/empty heartbeat timestamp";
            logger.error(errorMessage);
            return true;
        }
        return false;
    }
}
