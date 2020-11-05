package com.netflix.conductor.contribs.kafka.workers;

import com.netflix.conductor.contribs.kafka.config.KafkaPropertiesProvider;
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
    private static final int KEY_ERROR_BRANCH = 0;
    private static final int PROCESS_HEARTBEAT_BRANCH = 1;
    private static final int HEARTBEAT_ERROR_BRANCH = 2;
    private boolean inactiveWorkerMonitorRunning = false;
    private final Set<String> activeWorkers;
    private final Map<String, Status> workersStatus;
    private final Properties statusListenerProperties;
    private final WorkerHeartbeatSerde workerHeartbeatSerde;
    private final Timer timer;
    private final String workersHeartbeatTopic;
    private final long workerInactiveTimer;

    public ActiveWorkersMonitor(final Configuration configuration,
                                final KafkaPropertiesProvider kafkaPropertiesProvider){
        this.activeWorkers = new LinkedHashSet<>();
        this.workersStatus = new HashMap<>();
        this.timer = new Timer();
        this.workerHeartbeatSerde = new WorkerHeartbeatSerde();
        this.workersHeartbeatTopic = getWorkersHeartbeatTopic(configuration);
        this.statusListenerProperties = kafkaPropertiesProvider.getStreamsProperties("status-listener");
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
    public void addActiveWorker(final String worker) {
        startInActiveWorkerMonitor();   // This function will be skipped if its already started
        if (!activeWorkers.contains(worker)) {
            activeWorkers.add(worker);
            workersStatus.put(worker, new Status(workerInactiveTimer));
        } else {
            updateNumOfInstances(worker);
        }
    }

    /**
     * Receives and process heartbeats received from services/workers
     *
     * @param worker The name of the worker
     * @param workerHeartbeat Worker heartbeat object
     */
    private void receivedHeartbeat(final String worker, final WorkerHeartbeat workerHeartbeat){
        if (activeWorkers.contains(worker)){
            Status status = workersStatus.get(worker);
            status.updateLastHeartbeat(workerHeartbeat.getHeartbeatTimeStampMS(), workerInactiveTimer);
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
            inactiveWorkerMonitorRunning = false;
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
     * @param lastHeartbeat The time of last heartbeat
     * @return
     */
    private boolean isInActive(final String worker, final long lastHeartbeat){
        return lastHeartbeat >= workersStatus.get(worker).getTimeToNextHeartbeat();
    }

    /**
     * Starts the inactive worker monitor timer thread for frequently checking if a service is inactive
     * (Have not sent a heartbeat within a specific tomeframe)
     */
    private void startInActiveWorkerMonitor(){
        if (!inactiveWorkerMonitorRunning){
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
     * Get the number of instances for worker currently running
     *
     * @param worker The service name
     * @return The total of instances currently running for given worker
     */
    public int getTotalInstances(final String worker){
        if (isActive(worker)) {
            return workersStatus.get(worker).getNumOfInstances();
        }
        return 0;
    }

    /**
     * Update the number of instances running
     *
     * @param worker The service name
     */
    private void updateNumOfInstances(final String worker) {
        workersStatus.get(worker).incrementNumOfInstances();
    }

    /**
     * Removes an inactive worker from status collections
     *
     * @param worker The service name
     */
    public void removeInActiveWorker(final String worker){
        activeWorkers.remove(worker);
        workersStatus.remove(worker);
    }

    /**
     * Get workers heartbeat topic from configuration
     *
     * @param configuration Main configuration file for the Conductor application
     * @return Topic to subscribe for workers heartbeat signals
     */
    private String getWorkersHeartbeatTopic(final Configuration configuration){
        String topic = configuration.getProperty("worker.heartbeat.topic", "");
        if (topic == null){
            logger.error("Configuration missing for worker heartbeat topic.");
            throw new IllegalArgumentException("Configuration missing for worker heartbeat topic..");
        }
        return topic;
    }

    /**
     * Closes the inactive worker monitor timer thread
     */
    public void closeInActiveWorkerMonitor(){
        timer.cancel();
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
        Predicate<String, WorkerHeartbeat> keyError = (serviceName, heartbeat) -> serviceName.isEmpty();
        Predicate<String, WorkerHeartbeat> errorOccurred = (serviceName, heartbeat) -> heartbeat.isDeserializationErrorOccurred();
        Predicate<String, WorkerHeartbeat> processHeartbeatReceived = (serviceName, heartbeat) -> !heartbeat.isDeserializationErrorOccurred();
        KStream<String, WorkerHeartbeat>[] executeDept = statusStream.branch(keyError, processHeartbeatReceived,
                errorOccurred);
        executeDept[KEY_ERROR_BRANCH].foreach((k, v) -> logger.debug(v.getDeserializationError()));
        executeDept[PROCESS_HEARTBEAT_BRANCH].foreach(this::receivedHeartbeat);
        executeDept[HEARTBEAT_ERROR_BRANCH].foreach((k, v) -> logger.debug(v.getDeserializationError()));
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
     * A status object used to encapsulate the status of an active worker
     */
    private static class Status{

        private long lastHeartbeat = Long.MIN_VALUE;
        private long timeToNextHeartbeat = Long.MIN_VALUE;
        private int numOfInstances = 1;

        public Status(final long workerInactive){
            resetTimeToNextHeartbeat(lastHeartbeat, workerInactive);
        }

        /**
         * Sets thee latest heartbeat received and reset the inactive timer
         * @param heartbeatMs The time of heartbeat
         * @param workerInactiveTimer The specific length of time a heartbeat must be received from a worker/service to be considered active
         */
        public void updateLastHeartbeat(final long heartbeatMs, final long workerInactiveTimer){
            setLastHeartbeat(heartbeatMs);
            resetTimeToNextHeartbeat(lastHeartbeat, workerInactiveTimer);
        }

        /**
         * Gets the time until heartbeat must be received from a worker/service to be considered active
         * @return
         */
        public long getTimeToNextHeartbeat() {
            return timeToNextHeartbeat;
        }

        /**
         * Sets the time of the last heartbeat received
         * @param lastHeartbeat
         */
        public void setLastHeartbeat(final long lastHeartbeat) {
            this.lastHeartbeat = lastHeartbeat;
        }

        /**
         * Increase the num of instances by one
         */
        public void incrementNumOfInstances(){
            numOfInstances++;
        }

        /**
         * Gets thee total number of instances running
         * @return
         */
        public int getNumOfInstances(){
            return numOfInstances;
        }


        /**
         * Resets the time until a heartbeat must be received from a worker/service to be considered active
         * @param lastHeartbeat The time of heartbeat
         * @param workerInactiveTimer The specific length of time a heartbeat must be received from a worker/service to be considered active
         */
        private void resetTimeToNextHeartbeat(final long lastHeartbeat, final long workerInactiveTimer){
            this.timeToNextHeartbeat = lastHeartbeat + workerInactiveTimer;
        }
    }
}
