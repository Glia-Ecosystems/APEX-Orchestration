package com.netflix.conductor.contribs.kafka.streamsutil;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.netflix.conductor.contribs.kafka.model.ResponseContainer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ResponseContainerSerde implements Serde<ResponseContainer> {

    private static final Logger logger = LoggerFactory.getLogger(ResponseContainerSerde.class);
    // The method below allow a Gson object to be created for serialization purposes with the
    // capability to accept NaN - Non numeric numbers
    private final Gson gson = new GsonBuilder().serializeSpecialFloatingPointValues().create();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // This method is left empty until needed.
    }

    @Override
    public void close() {
        // This method is left empty until needed.
    }

    /**
     * Provides an override implementation to serialize a Response Container
     * object into a byte for publishing to Kafka
     *
     * @return Serialized RequestContainer object
     */
    @Override
    public Serializer<ResponseContainer> serializer() {
        return new Serializer<ResponseContainer>() {
            @Override
            public void configure(Map<String, ?> configs, boolean isKey) {
                // This method is left empty until needed.
            }

            @Override
            public byte[] serialize(String topic, ResponseContainer responseContainer) {
                return gson.toJson(responseContainer.getResponseData()).getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public void close() {
                // This method is left empty until needed.
            }
        };
    }

    @Override
    public Deserializer<ResponseContainer> deserializer() {
        return null;
    }
}
