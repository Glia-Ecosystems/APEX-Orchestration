package com.netflix.conductor.contribs.kafka.workers;

import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.contribs.kafka.config.KafkaPropertiesProvider;
import com.netflix.conductor.contribs.kafka.model.KafkaTopicsManager;
import com.netflix.conductor.contribs.kafka.model.RequestContainer;
import com.netflix.conductor.contribs.kafka.model.ResponseContainer;
import com.netflix.conductor.contribs.kafka.resource.handlers.ResourceHandler;
import com.netflix.conductor.contribs.kafka.streamsutil.WorkerHeartbeatSerde;
import com.netflix.conductor.core.config.Configuration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ActiveWorkersMonitor {

    private final Logger logger = LoggerFactory.getLogger(ActiveWorkersMonitor.class);
    private boolean inactiveWorkerMonitorRunning = false;
    private final Set<String> activeWorkers;
    private final Map<String, Status> workersStatus;
    private final Properties statusListenerProperties;
    private final WorkerHeartbeatSerde workerHeartbeatSerde;
    private final ResourceHandler resourceHandler;
    private final String workersHeartbeatTopic;
    private final long workerInactiveTimer;
    private Timer timer;

    public ActiveWorkersMonitor(final Configuration configuration,  final KafkaTopicsManager kafkaTopicsManager,
                                final ResourceHandler resourceHandler, final KafkaPropertiesProvider kafkaPropertiesProvider){
        this.resourceHandler = resourceHandler;
        this.activeWorkers = new LinkedHashSet<>();
        this.workersStatus = new HashMap<>();
        this.workerHeartbeatSerde = new WorkerHeartbeatSerde();
        this.workersHeartbeatTopic = getWorkersHeartbeatTopic(configuration, kafkaTopicsManager);
        // Adding unique ID to kafka stream, status listener application ID, to allow for each instance of Conductor
        // to receive each status (heartbeat) sent to the respective topic
        this.statusListenerProperties = kafkaPropertiesProvider.getStreamsProperties("status-listener-" + UUID.randomUUID().toString().substring(0, 7));
        this.workerInactiveTimer = configuration.getLongProperty("worker.inactive.ms", 120000);
        startWorkersStatusListenerStream();
    }

    /**
     * Add the name of an active worker to the active workers and
     * workersStatus collections. If worker is an additional instance of an active worker
     * set the number of instances of the worker currently running.
     *
     * This assist with Conductor knowing when a particular service is no longer
     * running (communicating with Conductor) so that thread (Worker Task Stream) can be closed
     *
     * @param worker The unique (key) service name
     */
    public void addActiveWorker(final String worker, final String taskName) {
        startInActiveWorkerMonitor();   // This function will be skipped if its already started
        activeWorkers.add(worker);
        workersStatus.put(worker, new Status(taskName, workerInactiveTimer));
        logger.debug("Added {} worker to collection of Active Workers", worker);
    }

    /**
     * Removes an inactive worker from status collections
     *
     * @param worker The service name
     */
    public void removeInActiveWorker(final String worker){
        unregisterWorker(workersStatus.get(worker).getTaskName());
        activeWorkers.remove(worker);
        workersStatus.remove(worker);
        logger.debug("Removed inActive {} worker from collection of Active Workers", worker);
    }

    /**
     * Makes a request to conductor to get a batch of tasks in the worker/service queue
     *
     * @param taskName The service task name
     */
    private void unregisterWorker(final String taskName){
        String path = "/metadata/taskdefs/" + taskName;
        ResponseContainer responseContainer = resourceHandler.processRequest(new RequestContainer("", path, "DELETE", ""));
        if (responseContainer.getStatus() == 200){
            logger.debug("Unregistered Task Definition: {}", taskName);
        } else {
            logger.debug("Error occurred unregistering task definition for {}. Error: {}",
                    taskName, responseContainer.getResponseErrorMessage());
        }
    }

    /**
     *  Get all registered task definitions
     *
     * @return List of registered task definitions
     */
    @SuppressWarnings("unchecked")
    public List<TaskDef> getExistingTaskDefinitions() {
        ResponseContainer responseContainer = resourceHandler.processRequest(new RequestContainer("", "/metadata/taskdefs", "GET", ""));
        if (responseContainer.getStatus() == 200) {
            return (List<TaskDef>) responseContainer.getResponseEntity();
        } else {
            logger.debug("Error occurred getting registered task definitions. Error: {}",
                    responseContainer.getResponseErrorMessage());
            return new ArrayList<>();
        }
    }


    /**
     * Checks if any workers/services are in active, by checking if a heartbeat has been received
     * from the worker/service lately
     */
    private void checkIfInActive(){
        long now = System.currentTimeMillis();
        for (String worker: activeWorkers){
            if (isInActive(worker, now)){
                removeInActiveWorker(worker);
            }
        }
        if (activeWorkers.isEmpty()){
            closeInActiveWorkerMonitor();
            inactiveWorkerMonitorRunning = false;
        }
    }

    /**
     * Starts the inactive worker monitor timer thread for frequently checking if a service is inactive
     * (Have not sent a heartbeat within a specific timeframe)
     */
    private void startInActiveWorkerMonitor(){
        if (!inactiveWorkerMonitorRunning){
            timer = new Timer();  // Instantiation of timer here allows for a timer to be canceled and started dynamically
            TimerTask checkForInActiveWorkers = new TimerTask() {
                @Override
                public void run() {
                    checkIfInActive();
                }
            };
            timer.scheduleAtFixedRate(checkForInActiveWorkers, workerInactiveTimer, workerInactiveTimer);
            inactiveWorkerMonitorRunning = true;
        }
    }

    /**
     * Receives and process heartbeats received from services/workers
     *
     * @param worker The name of the worker
     * @param workerHeartbeat Worker heartbeat object
     */
    private void receivedHeartbeat(final String worker, final WorkerHeartbeat workerHeartbeat){
        // Only consider workers/service that successfully registered themselves with Conductor
        if (activeWorkers.contains(worker)){
            Status status = workersStatus.get(worker);
            status.updateLastHeartbeat(workerHeartbeat.getHeartbeatTimeStampMS(), workerInactiveTimer);
        }
    }

    /**
     * Get an indication of if a worker is still active
     *
     * @param worker  The service name
     * @return  Indication of if a worker is still active
     */
    public boolean isActive(final String worker){
        return activeWorkers.contains(worker);
    }

    /**
     * Checks if worker/service is inactive
     *
     * @param worker The service name
     * @param now Current time in milliseconds
     * @return Indicator if a worker is inactive
     */
    private boolean isInActive(final String worker, final long now){
        return now >= workersStatus.get(worker).getTimeToNextHeartbeat();
    }

    /**
     * Get workers heartbeat topic from configuration
     *
     * @param configuration Main configuration file for the Conductor application
     * @return Topic to subscribe for workers heartbeat signals
     */
    private String getWorkersHeartbeatTopic(final Configuration configuration, final KafkaTopicsManager kafkaTopicsManager){
        String topic = configuration.getProperty("workers.heartbeat.topic", "");
        if (topic.equals("")){
            logger.error("Configuration missing for worker heartbeat topic.");
            throw new IllegalArgumentException("Configuration missing for worker heartbeat topic..");
        }
        kafkaTopicsManager.createTopic(topic);
        return topic;
    }

    /**
     * Creates the topology for listening and monitoring active workers status
     *
     * @return A kafka task streams topology for listening to heartbeats from workers
     */
    @SuppressWarnings("unchecked")
    private Topology buildStatusListenerTopology(){
        logger.info("Building Kafka Streams Topology for listening to workers heartbeat");
        // Build kafka streams topology
        StreamsBuilder builder = new StreamsBuilder();
        // Parent Node
        // Source Node (Responsible for consuming the records from a given topic, that will be processed)
        KStream<String, WorkerHeartbeat> statusStream = builder.stream(workersHeartbeatTopic,
                Consumed.with(Serdes.String(), workerHeartbeatSerde))
                .peek((k, v) -> logger.debug("Worker {} sent heartbeat", k));
        // Branch Processor Node
        // Each record is matched against the given predicates in the order that they're provided.
        // The branch processor will assign records to a stream on the first match.
        // WARNING: No attempts are made to match additional predicates.
        // If a no key given error occurs, send error to client who made initial request
        // Filters processing of request, if any key or value errors occur when containerising request
        // send error to service, else process request
        // NOTE: Branch Processor Node is a list of predicates that are indexed to form the following process
        // if predicate is true.
        Predicate<String, WorkerHeartbeat> keyError = (serviceName, heartbeat) -> serviceName.isEmpty();
        Predicate<String, WorkerHeartbeat> errorOccurred = (serviceName, heartbeat) -> heartbeat.isDeserializationErrorOccurred();
        Predicate<String, WorkerHeartbeat> processHeartbeatReceived = (serviceName, heartbeat) -> !heartbeat.isDeserializationErrorOccurred();
        KStream<String, WorkerHeartbeat>[] executeDept = statusStream.branch(keyError, errorOccurred, processHeartbeatReceived);
        executeDept[0].foreach((k, v) -> logger.debug(v.getDeserializationError()));
        executeDept[1].foreach((k, v) -> logger.debug(v.getDeserializationError()));
        executeDept[2].foreach(this::receivedHeartbeat);
        return builder.build();
    }

    /**
     * Create a KafkaStreams object containing the built topology and properties file
     * for receiving heartbeat from workers via kafka streams.
     *
     * @param streamsTopology A topology object containing the structure of the kafka streams
     */
    private KafkaStreams buildKafkaStream(Topology streamsTopology) {
        KafkaStreams builtStream = new KafkaStreams(streamsTopology, statusListenerProperties);
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
     * Builds and start the kafka stream topology and kafka stream object for receiving heartbeats from workers.
     */
    public void startWorkersStatusListenerStream() {
        // Build the topology
        Topology statusListenerTopology = buildStatusListenerTopology();
        logger.debug("Status Listener Topology Description: {}", buildStatusListenerTopology().describe());
        // Build/Create Kafka Streams object for receiving and processing workers status via kafka streams
        KafkaStreams statusListenerStream = buildKafkaStream(statusListenerTopology);
        logger.info("Starting Kafka Streams for receiving heartbeats from workers to Conductor");
        statusListenerStream.start();
        // Add shutdown hook to respond to SIGTERM and gracefully close the Streams application.
        Runtime.getRuntime().addShutdownHook(new Thread(statusListenerStream::close));
    }

    /**
     * Closes the inactive worker monitor timer thread
     */
    public void closeInActiveWorkerMonitor(){
        timer.cancel();
    }

    /**
     * A status object used to encapsulate the status of an active worker
     */
    private static class Status{

        private final String taskName;
        private long lastHeartbeat;
        private long timeToNextHeartbeat;

        public Status(final String taskName, final long workerInactive){
            this.taskName = taskName;
            this.lastHeartbeat = System.currentTimeMillis();
            resetTimeToNextHeartbeat(workerInactive);
        }

        /**
         * Sets thee latest heartbeat received and reset the inactive timer
         * @param heartbeatMs The time of heartbeat
         * @param workerInactiveTimer The specific length of time a heartbeat must be received from a worker/service to be considered active
         */
        public void updateLastHeartbeat(final long heartbeatMs, final long workerInactiveTimer){
            setLastHeartbeat(heartbeatMs);
            resetTimeToNextHeartbeat(workerInactiveTimer);
        }

        /**
         * Gets the time until heartbeat must be received from a worker/service to be considered active
         * @return Time until next expected heartbeat
         */
        public long getTimeToNextHeartbeat() {
            return timeToNextHeartbeat;
        }

        /**
         * Sets the time of the last heartbeat received
         * @param receivedHeartbeatMs The received heartbeat from a worker
         */
        public void setLastHeartbeat(final long receivedHeartbeatMs) {
            this.lastHeartbeat = receivedHeartbeatMs;
        }

        /**
         * Gets the task name of the service task definition
         * @return The task name
         */
        public String getTaskName() { return taskName; }

        /**
         * Resets the time until a heartbeat must be received from a worker/service to be considered active
         * @param workerInactiveTimer The specific length of time a heartbeat must be received from a worker/service to be considered active
         */
        private void resetTimeToNextHeartbeat(final long workerInactiveTimer){
            this.timeToNextHeartbeat = this.lastHeartbeat + workerInactiveTimer;
        }
    }
}
