package com.netflix.conductor.contribs.kafka.model;

import com.netflix.conductor.contribs.kafka.config.KafkaPropertiesProvider;
import com.netflix.conductor.core.config.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;

public class HeartbeatCoordinator {

    private final Logger logger = LoggerFactory.getLogger(HeartbeatCoordinator.class);
    private final Timer timer = new Timer();

    public HeartbeatCoordinator(final Configuration configuration, final KafkaPropertiesProvider kafkaPropertiesProvider,
                                final KafkaTopicsManager kafkaTopicsManager){
        TimerTask heartbeat = new Heartbeat(configuration, kafkaPropertiesProvider, kafkaTopicsManager);
        startHeartbeat(heartbeat, configuration.getLongProperty("heartbeat.interval.ms", 60000));
    }

    /**
     * Starts the heartbeat thread for Conductor
     *
     * @param heartbeat The main class  for producing and publishing heartbeats
     * @param heartbeatIntervalMs The period between successive heartbeats
     */
    public void startHeartbeat(final TimerTask heartbeat, final long heartbeatIntervalMs){
        logger.info("Starting heartbeat");
        // 45 secs delay before the first heartbeat is sent
        timer.scheduleAtFixedRate(heartbeat, 45000, heartbeatIntervalMs);
    }

    /**
     * Closes the heartbeat thread of Conductor
     */
    public void stopHeartbeat(){
        logger.info("Stopping heartbeat");
        timer.cancel();
    }
}

