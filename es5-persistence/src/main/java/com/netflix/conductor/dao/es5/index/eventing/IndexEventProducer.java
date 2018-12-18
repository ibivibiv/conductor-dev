package com.netflix.conductor.dao.es5.index.eventing;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.run.Workflow;

public class IndexEventProducer extends Producer {

	public IndexEventProducer(String exchange, String routingKey, String hostname, int port)
			throws IOException, TimeoutException {
		super(exchange, routingKey, hostname, port);
		// TODO Auto-generated constructor stub
	}

	public void sendWorkflow(String indexName, String id, Workflow workflow) throws IOException {
		String message = "";
		channel.basicPublish(exchange, routingKey, null, message.getBytes());
	}

	public void sendTask(String indexName, String id, Task task) throws IOException {
		String message = "";
		channel.basicPublish(exchange, routingKey, null, message.getBytes());
	}

}
