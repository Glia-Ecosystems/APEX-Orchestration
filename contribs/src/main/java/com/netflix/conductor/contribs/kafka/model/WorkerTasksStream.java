package com.netflix.conductor.contribs.kafka.model;

import com.google.gson.Gson;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.contribs.kafka.resource.handlers.ResourceHandler;
import com.netflix.conductor.contribs.kafka.streamsutil.KafkaStreamsDeserializationExceptionHandler;
import com.netflix.conductor.contribs.kafka.streamsutil.RequestContainerSerde;
import com.netflix.conductor.contribs.kafka.streamsutil.ResponseContainerSerde;
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
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WorkerTasksStream implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(WorkerTasksStream.class);
    private static final int KEY_ERROR_BRANCH = 0;
    private static final int REGISTER_BRANCH = 1;
    private static final int VALUE_ERROR_BRANCH = 2;
    private final KafkaProducer<String, String> producer;
    private final Properties responseStreamProperties;
    private final ResourceHandler resourceHandler;
    private final ExecutorService taskPublishPool;
    private final Map<String, Integer> activeWorkers;
    // Create custom Serde objects for processing records
    private final RequestContainerSerde requestContainerSerde;
    private final ResponseContainerSerde responseContainerSerde;
    private final String worker;
    private final String taskName;
    private final String tasksTopic;
    private final String updateTopic;
    private final String statusTopic;
    private final int pollBatchSize;
    private final Gson gson;
    private KafkaStreams tasksStream;


    public WorkerTasksStream(final ResourceHandler resourceHandler, final Properties responseStreamProperties,
                             final Properties producerProperties, final Map<String, Integer>activeWorkers,
                             final String worker, final String taskName, final Map<String, String> topics,
                             final int pollBatchSize){
        this.resourceHandler = resourceHandler;
        this.worker = worker;
        this.taskName = taskName;
        this.tasksTopic = topics.get("taskTopic");
        this.updateTopic = topics.get("updateTopic");
        this.statusTopic = topics.get("statusTopic");
        this.responseStreamProperties = responseStreamProperties;
        this.activeWorkers = activeWorkers;
        this.gson = new Gson();
        this.requestContainerSerde = new RequestContainerSerde(new JsonMapperProvider().get());
        this.responseContainerSerde = new ResponseContainerSerde();
        this.producer = new KafkaProducer<>(producerProperties);
        this.pollBatchSize = pollBatchSize;
        this.taskPublishPool = Executors.newFixedThreadPool(pollBatchSize);

    }

    /**
     * Creates a topology for processing tasks between workers and Conductor
     *
     * It is assumed that the topics provided is already configured the kafka cluster.
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
        Predicate<String, RequestContainer> keyError = (workerName, request) -> workerName.isEmpty();
        Predicate<String, RequestContainer> continueTaskStream = (workerName, request) ->
                !request.isDeserializationErrorOccurred();
        Predicate<String, RequestContainer> errorOccurred = (workerName, request) ->
                request.isDeserializationErrorOccurred();
        KStream<String, RequestContainer>[] executeDept = taskStream.branch(keyError, continueTaskStream,
                errorOccurred);
        KStream<String, ResponseContainer> processedKeyError = executeDept[KEY_ERROR_BRANCH].
                mapValues(KafkaStreamsDeserializationExceptionHandler::processKeyError);
        KStream<String, ResponseContainer> processedTask = executeDept[REGISTER_BRANCH].
                mapValues(resourceHandler::processRequest);
        KStream<String, ResponseContainer> processedValueError = executeDept[VALUE_ERROR_BRANCH].
                mapValues(KafkaStreamsDeserializationExceptionHandler::processValueError);
        processedTask.to(statusTopic, Produced.with(Serdes.String(), responseContainerSerde));
        processedKeyError.to(statusTopic, Produced.with(Serdes.String(), responseContainerSerde));
        processedValueError.to(statusTopic, Produced.with(Serdes.String(), responseContainerSerde));
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
        while (activeWorkers.containsKey(worker)) {
            ResponseContainer responseContainer = batchPollTasks();
            List<Task> batchTasks = (List<Task>) responseContainer.getResponseEntity();
            if (batchTasks != null && !batchTasks.isEmpty()){
                // Use multithreading to publish batch of tasks to task queue topic for worker/service
                batchTasks.forEach(task -> taskPublishPool.execute(() -> publishTask(worker, gson.toJson(task))));
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
        return resourceHandler.processRequest(new RequestContainer(path, "GET", ""));
    }

    /**
     * Publish the task to the task queue topic for worker/service.
     *
     * @param service Key (service name) of the record to be publish
     * @param task Value (task) of the record to be publish
     */
    public void publishTask(final String service, final String task) {
        final RecordMetadata metadata;
        final ProducerRecord<String, String> record = new ProducerRecord<>(tasksTopic, service, task);
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
            Thread.sleep(10); // 10 millisecond thread sleep
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
