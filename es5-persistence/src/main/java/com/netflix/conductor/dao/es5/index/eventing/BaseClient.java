package com.netflix.conductor.dao.es5.index.eventing;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Represents a connection with a queue
 * 
 * @author syntx
 *
 */
public abstract class BaseClient {

	protected Channel channel;
	protected Connection connection;
	
	protected int port;

	public BaseClient(String host, int port)
			throws IOException, TimeoutException {
		

		// Create a connection factory
		ConnectionFactory factory = new ConnectionFactory();

		// hostname of your rabbitmq server
		factory.setHost(host);
		factory.setPort(port);

		// getting a connection
		connection = factory.newConnection();

		// creating a channel
		channel = connection.createChannel();

		// declaring a queue for this channel. If queue does not exist,
		// it will be created on the server.
		//channel.queueDeclare(queue, false, false, false, null);

		//channel.queueBind(queue, exchange, routingKey);
	}

	/**
	 * Close channel and connection. Not necessary as it happens implicitly any way.
	 * 
	 * @throws IOException
	 * @throws TimeoutException
	 */
	public void close() throws IOException, TimeoutException {
		this.channel.close();
		this.connection.close();
	}
}
