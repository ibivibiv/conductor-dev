package com.netflix.conductor.contribs;

/**
 * Copyright 2016 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.ProvidesIntoMap;
import com.google.inject.multibindings.StringMapKey;
import com.google.inject.name.Named;
import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.contribs.queue.QueueManager;
import com.netflix.conductor.contribs.queue.rabbit.RabbitObservableQueue;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.EventQueueProvider;
import com.netflix.conductor.core.events.rabbit.RabbitEventQueueProvider;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class RabbitModule extends AbstractModule {
    private static final String DEFAULT_EXCHANGE = "default.exchange:";
	private static Logger logger = LoggerFactory.getLogger(RabbitModule.class);

    @Override
    protected void configure() {
        bind(QueueManager.class).asEagerSingleton();
        logger.info("Rabbit module configured.");
    }

    @ProvidesIntoMap
    @StringMapKey("rabbit")
    @Singleton
    @Named("EventQueueProviders")
    public EventQueueProvider getRabbitEventQueueProvider(Configuration configuration) {
        return new RabbitEventQueueProvider(configuration);
    }

    @Provides
    public Map<Task.Status, ObservableQueue> getQueues(Configuration config) throws IOException, TimeoutException {

        String stack = "";
        if(config.getStack() != null && config.getStack().length() > 0) {
            stack = config.getStack() + "_";
        }
        Task.Status[] statuses = new Task.Status[]{Task.Status.COMPLETED, Task.Status.FAILED};
        Map<Task.Status, ObservableQueue> queues = new HashMap<>();
        for(Task.Status status : statuses) {
            String queueNameWithExchange = config.getProperty("workflow.listener.queue.prefix." + status.name(), DEFAULT_EXCHANGE + config.getAppId() + "_rabbit_notify_" + stack + status.name());
            ObservableQueue  queue = new RabbitObservableQueue(queueNameWithExchange, config);
            queues.put(status, queue);
        }
        return queues;
    }

}
