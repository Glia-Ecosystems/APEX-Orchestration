package com.netflix.conductor.contribs.kafka.streamsutil;

import com.netflix.conductor.contribs.kafka.model.RequestContainer;
import com.netflix.conductor.contribs.kafka.model.ResponseContainer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;

public class KafkaStreamsDeserializationExceptionHandler extends LogAndContinueExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsDeserializationExceptionHandler.class);

    /**
     * Exception handler for errors that occur during deserialization of records
     * @param context Context of a Processor object
     * @param record Record received from Kafka Streams
     * @param exception Exception that occurred during deserialization
     * @return DeserializationHandlerResponse object to inform kafka streams to continue processing further records
     */
    @Override
    public DeserializationHandlerResponse handle(ProcessorContext context, ConsumerRecord<byte[], byte[]> record, Exception exception) {
        logger.error("Exception caught during Deserialization, taskId: {}, topic: {}, partition: {}, offset: {}",
                context.taskId(), record.topic(), record.partition(), record.offset(), exception);
        return DeserializationHandlerResponse.CONTINUE;
    }

    /**
     * Process the key exception into a response container to be return to the client who made the request
     *
     * @param requestContainer Contains all the needed information for processing the request
     * @return Response of the exception that occurred for client
     */
    public static ResponseContainer processKeyError(RequestContainer requestContainer){
        // Create a request and response container
        final ResponseContainer response = new ResponseContainer(requestContainer);
        response.setStatus(400);
        response.setResponseEntity(Response.Status.BAD_REQUEST);
        response.setResponseErrorMessage("Key can not be null");
        return response;
    }

    /**
     * Process the value exception into a response container to be return to the client who made the request
     *
     * @param requestContainer Contains all the needed information for processing the request
     * @return Response of the exception that occurred for client
     */
    public static ResponseContainer processValueError(RequestContainer requestContainer){
        // Create a request and response container
        final ResponseContainer response = new ResponseContainer(requestContainer);
        response.setStatus(400);
        response.setResponseEntity(Response.Status.BAD_REQUEST);
        response.setResponseErrorMessage(requestContainer.getDeserializationError());
        return response;
    }

    /***
     * Process the unique Kafka Streams topic URI exception into a response container to be return to the client who
     * made the request
     *
     * @param requestContainer Contains all the needed information for processing the request
     * @return Response of the exception that occurred for client
     */
    public static ResponseContainer processUniqueURIError(RequestContainer requestContainer){
        // Create a request and response container
        final ResponseContainer response = new ResponseContainer(requestContainer);
        response.setStatus(400);
        response.setResponseEntity(Response.Status.BAD_REQUEST);
        response.setResponseErrorMessage("URI requested is not allowed for this Kafka Stream Topic");
        return response;
    }
}
