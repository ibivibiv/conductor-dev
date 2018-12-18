package com.netflix.conductor.dao.es5.index.eventing;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * The producer endpoint that writes to the queue.
 * 
 * @author syntx
 *
 */
public class Producer extends BaseClient {

	protected String exchange;
	protected String routingKey;

	public Producer(String exchange, String routingKey, String hostname, int port)
			throws IOException, TimeoutException {
		super(hostname, port);
		this.exchange = exchange;
		this.routingKey = routingKey;
	}

	
}
