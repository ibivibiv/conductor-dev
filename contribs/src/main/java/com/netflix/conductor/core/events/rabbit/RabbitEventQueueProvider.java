package com.netflix.conductor.core.events.rabbit;

import com.google.inject.Inject;
import com.netflix.conductor.contribs.queue.rabbit.RabbitObservableQueue;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.EventQueueProvider;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.core.execution.ApplicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Singleton;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

@Singleton
public class RabbitEventQueueProvider implements EventQueueProvider {

    private static Logger logger = LoggerFactory.getLogger(RabbitEventQueueProvider.class);

    protected Map<String, RabbitObservableQueue> queues = new ConcurrentHashMap<>();

    private Configuration config;

    @Inject
    public RabbitEventQueueProvider(Configuration config) {
        this.config = config;
        logger.info("Rabbit Event Queue Provider initialized.");
    }

    @Override
    public ObservableQueue getQueue(String queueURI) {
        return queues.computeIfAbsent(queueURI, q -> {
            try {
                return new RabbitObservableQueue(queueURI, config);
            } catch (IOException e) {
                throw new ApplicationException(ApplicationException.Code.INVALID_INPUT, "Failed To Publish To The Queue Due To Invalid Input");
            } catch (TimeoutException e) {
                throw new ApplicationException(ApplicationException.Code.NOT_FOUND, "Failed To Publish To The Queue Due to Queue Not Found");
            }

        });
    }
}
