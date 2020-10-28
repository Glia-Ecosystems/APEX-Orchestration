package com.netflix.conductor.contribs.kafka.workers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class ActiveWorkersMonitor {

    private final Logger logger = LoggerFactory.getLogger(ActiveWorkersMonitor.class);
    private final Set<String> activeWorkers;
    private final Map<String, Status> workersStatus;

    public ActiveWorkersMonitor(){
        this.activeWorkers = new LinkedHashSet<>();
        this.workersStatus = new HashMap<>();
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
        if (!activeWorkers.contains(worker)) {
            activeWorkers.add(worker);
            workersStatus.put(worker, new Status());
        } else {
            updateNumOfInstances(worker);
        }
    }

    public void removeInActiveWorker(final String worker){
        activeWorkers.remove(worker);
        workersStatus.remove(worker);
    }

    /**
     * A status object used to encapsulate the status of an active worker
     */
    private static class Status{

        private long lastHeartbeat = Long.MAX_VALUE;
        private int numOfInstances = 1;

        /**
         *
         * @return
         */
        public long getLastHeartbeat() {
            return lastHeartbeat;
        }

        /**
         *
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
         *
         * @return
         */
        public int getNumOfInstances(){
            return numOfInstances;
        }

        public void setNumOfInstances(int numOfInstances) {
            this.numOfInstances = numOfInstances;
        }
    }
}
