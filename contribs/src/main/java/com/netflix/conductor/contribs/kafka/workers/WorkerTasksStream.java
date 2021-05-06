package com.netflix.conductor.contribs.kafka.workers;

import com.google.gson.Gson;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.contribs.kafka.model.RequestContainer;
import com.netflix.conductor.contribs.kafka.model.ResponseContainer;
import com.netflix.conductor.contribs.kafka.resource.handlers.ResourceHandler;
import com.netflix.conductor.contribs.kafka.streamsutil.KafkaStreamsDeserializationExceptionHandler;
import com.netflix.conductor.contribs.kafka.streamsutil.RequestContainerSerde;
import com.netflix.conductor.core.execution.ApplicationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class WorkerTasksStream implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(WorkerTasksStream.class);
    private final KafkaProducer<String, String> producer;
    private final Properties responseStreamProperties;
    private final ResourceHandler resourceHandler;
    private final ActiveWorkersMonitor activeWorkersMonitor;
    // Create custom Serde objects for processing records
    private final RequestContainerSerde requestContainerSerde;
    private final String worker;
    private final String taskName;
    private final String tasksTopic;
    private final String updateTopic;
    private final int pollBatchSize;
    private final int taskPollingInterval;
    private final Gson gson;
    private KafkaStreams tasksStream;


    public WorkerTasksStream(final ResourceHandler resourceHandler, final Properties responseStreamProperties,
                             final Properties producerProperties, final ActiveWorkersMonitor activeWorkersMonitor,
                             final String worker, final String taskName, final Map<String, String> topics,
                             final int pollBatchSize, final int taskPollingInterval){
        this.resourceHandler = resourceHandler;
        this.worker = worker;
        this.taskName = taskName;
        this.tasksTopic = topics.get("taskTopic");
        this.updateTopic = topics.get("updateTopic");
        this.responseStreamProperties = responseStreamProperties;
        this.activeWorkersMonitor = activeWorkersMonitor;
        this.gson = new Gson();
        this.requestContainerSerde = new RequestContainerSerde();
        this.producer = new KafkaProducer<>(producerProperties);
        this.pollBatchSize = pollBatchSize;
        this.taskPollingInterval = taskPollingInterval;
    }

    /**
     * Creates a topology for processing tasks between workers and Conductor
     *
     * It is assumed that the topics provided is already configured in the kafka cluster.
     *
     * @return A kafka task streams topology for processing tasks
     */
    @SuppressWarnings("unchecked")
    private Topology buildTaskStreamTopology() {
        logger.info("Building Kafka Update Task Stream Topology for {}", worker);
        // Build kafka streams topology
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, RequestContainer> taskStream = builder.stream(updateTopic, Consumed.with(Serdes.String(),
                requestContainerSerde));
        Predicate<String, RequestContainer> errorOccurred = (key, request) ->
                request.isDeserializationErrorOccurred();
        Predicate<String, RequestContainer> uniqueURIError = (key, request) ->
                !request.getResourceURI().contains("/tasks");
        Predicate<String, RequestContainer> continueTaskStream = (key, request) ->
                !request.isDeserializationErrorOccurred();
        // NOTE: Branch Processor Node is a list of predicates that are indexed to form the following process
        // if predicate is true.
        KStream<String, RequestContainer>[] executeDept = taskStream.branch(errorOccurred, uniqueURIError,
                continueTaskStream);
        KStream<String, ResponseContainer> processedValueError = executeDept[0].
                mapValues(KafkaStreamsDeserializationExceptionHandler::processValueError);
        KStream<String, ResponseContainer> processedURIError = executeDept[1].
                mapValues(KafkaStreamsDeserializationExceptionHandler::processUniqueURIError);
        KStream<String, ResponseContainer> processedTask = executeDept[2].mapValues(resourceHandler::processRequest);
        // Log errors, if any occurs
        processedValueError.foreach((k, v) -> logger.error("Value error received from {} service while attempting to" +
                        " update or acknowledge task.Status: {}. Error: {}. Error Message: {}",
                v.getKey(), v.getStatus(), v.getResponseEntity(), v.getResponseErrorMessage()));
        processedURIError.foreach((k, v) -> logger.error("URI error received from {} service while attempting to" +
                        " update or acknowledge task.Status: {}. Error: {}. Error Message: {}",
                v.getKey(), v.getStatus(), v.getResponseEntity(), v.getResponseErrorMessage()));
        processedTask.filter((k, v) -> v.getStatus() != 200)
                .foreach((k, v) -> logger.error("Error occurred processing update or acknowledge task request from {} service." +
                                "Status: {}. Error: {}. Error Message: {}", v.getKey(), v.getStatus(), v.getResponseEntity(),
                        v.getResponseErrorMessage()));
        return builder.build();
    }

    /**
     * Create a KafkaStreams object containing the built topology and properties file
     * for processing requests to the Conductor API via kafka streams.
     *
     * @param streamsTopology A topology object containing the structure of the kafka streams
     */
    private KafkaStreams buildKafkaStream(final Topology streamsTopology) {
        KafkaStreams builtStream = new KafkaStreams(streamsTopology, responseStreamProperties);
        // here you should examine the throwable/exception and perform an appropriate action!

        // Note on exception handling.
        // Exception handling can be implemented via implementing interfaces such as
        // ProductionExceptionHandler or overriding/extending classes such as LogAndContinueExceptionHandler
        // for deserialization exceptions.
        // ProductionExceptionHandler handles only exceptions on producer (exceptions occurring when sending messages
        // via producer), it will not handle exceptions during processing of stream methods (mapValues(), branch(), etc.)
        // You will need to wrap these methods in try / catch blocks.
        // For consumer side, kafka streams automatically retry consuming record, because offset will not be changed
        // until record is consumed and processed. Use setUncaughtExceptionHandler to log exception
        // or send message to a failure topic.
        builtStream.setUncaughtExceptionHandler((Thread thread, Throwable throwable) ->
                // You can make it restart the stream, but you have to make sure that this thread is
                // destroyed once a new thread is spawned to restart the kafka streams
                logger.error(String.valueOf(throwable)));
        return builtStream;
    }

    /**
     *  While the worker/service is active, poll tasks from queue in Conductor and publish to task queue topic for
     *  worker.
     */
    @SuppressWarnings("unchecked")
    private void pollAndPublish(){
        while (activeWorkersMonitor.isActive(worker)) {
            ResponseContainer responseContainer = batchPollTasks();
            List<Task> batchTasks = (List<Task>) responseContainer.getResponseEntity();
            if (batchTasks != null && !batchTasks.isEmpty()){
                // Publish batch of tasks to task queue topic for worker/service
                batchTasks.forEach(task -> publishTask(gson.toJson(task)));
            }
            pollingInterval();
        }
    }

    /**
     * Makes a request to conductor to get a batch of tasks in the worker/service queue
     *
     * @return Response from the resource contained in a response container object
     */
    private ResponseContainer batchPollTasks(){
        String path = "/tasks/poll/batch/" + taskName + "?count=" + pollBatchSize;
        return resourceHandler.processRequest(new RequestContainer("", path, "GET", ""));
    }

    /**
     * Publish the task to the task queue topic for worker/service.
     *
     * @param task Value (task) of the record to be publish
     */
    public void publishTask(final String task) {
        final RecordMetadata metadata;
        final ProducerRecord<String, String> record = new ProducerRecord<>(tasksTopic, task);
        try {
            metadata = producer.send(record).get();
            final String producerLogging = "Producer Record: key " + record.key() + ", value " + record.value() +
                    ", partition " + metadata.partition() + ", offset " + metadata.offset();
            logger.debug(producerLogging);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Publish task to kafka topic {} failed with an error: {}", tasksTopic, e.getMessage(), e);
        } catch (final ExecutionException e) {
            logger.error("Publish task to kafka topic {} failed with an error: {}", tasksTopic, e.getMessage(), e);
            throw new ApplicationException(ApplicationException.Code.INTERNAL_ERROR, "Failed to publish the event");
        }
    }

    /**
     * Builds and start the kafka stream topology and kafka stream object for processing tasks
     * between workers and Conductor API.
     */
    private void startTaskStream(){
        // Build the topology
        Topology taskStreamTopology = buildTaskStreamTopology();
        logger.debug("{} Update Task Stream Topology Description: {}", worker, taskStreamTopology.describe());
        tasksStream = buildKafkaStream(taskStreamTopology);
        tasksStream.start();
        // Add shutdown hook to respond to SIGTERM and gracefully close the Streams application.
        Runtime.getRuntime().addShutdownHook(new Thread(tasksStream::close));
    }

    /**
     * Execute a Thread sleep for the established time
     */
    private void pollingInterval(){
        // Thread.sleep function is executed so that a consumed message is not sent
        // to Conductor before the server is started
        try {
            Thread.sleep(taskPollingInterval);
        } catch (final InterruptedException e) {
            // Restores the interrupt by the InterruptedException so that caller can see that
            // interrupt has occurred.
            Thread.currentThread().interrupt();
            logger.error("Error occurred while trying to sleep Thread. {}", e.getMessage());
        }
    }

    /**
     * Closing of all connections to Kafka Producer/Streams
     */
    public void closeAllKafkaConnections() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
        if (tasksStream != null) {
            tasksStream.close();
        }
    }


    /**
     * Creates a separate thread from the main worker streams thread for using Kafka Streams to
     * process tasks between workers and Conductor API
     */
    @Override
    public void run() {
        try{
            startTaskStream();
            pollAndPublish();
        } catch (final Exception e) {
            logger.error("WorkerTasksStream.pollAndPublish(), exiting due to error! {}", e.getMessage());
        }
        finally {
            try{
                closeAllKafkaConnections();
            } catch (final Exception e){
                logger.error("WorkerTasksStream.closeAllKafkaConnections(), exiting due to error! {}", e.getMessage());
            }
        }
    }
}
