package com.netflix.conductor.contribs.kafka;

import com.netflix.conductor.contribs.kafka.resource.handlers.ResourceHandler;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import javax.inject.Inject;
import java.util.List;

public class KafkaStreamsWorkersObservableQueue implements ObservableQueue, Runnable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamsWorkersObservableQueue.class);
    private final ResourceHandler resourceHandler;

    @Inject
    public KafkaStreamsWorkersObservableQueue(final ResourceHandler resourceHandler, final Configuration configuration) {
        this.resourceHandler = resourceHandler;
    }

    @Override
    public Observable<Message> observe() {
        return null;
    }

    @Override
    public String getType() {
        return null;
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getURI() {
        return null;
    }

    @Override
    public List<String> ack(List<Message> messages) {
        return null;
    }

    @Override
    public void publish(List<Message> messages) {

    }

    @Override
    public void setUnackTimeout(Message message, long unackTimeout) {

    }

    @Override
    public long size() {
        return 0;
    }

    @Override
    public void run() {

    }
}
