package com.netflix.conductor.contribs.queue.rabbit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.netflix.conductor.core.config.Configuration;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.metrics.Monitors;
import com.rabbitmq.client.*;
import io.nats.client.NUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;


public class RabbitObservableQueue implements ObservableQueue {

    private static Logger logger = LoggerFactory.getLogger(RabbitObservableQueue.class);

    private static final String QUEUE_TYPE = "rabbit";
    private static final int RABBIT_QUEUE_DEFAULTPOLLINGINTERVAL = 10000;
    private static final int RABBIT_QUEUE_DEFAULTLONGPOLLTIMEOUT = 1000;
    private static final int RABBIT_QUEUE_DEFAULTPOLLCOUNT = 10;
    private static final int RABBIT_PORT_DEFAULTPORT = 10;
    private static final String RABBIT_CONDUCTOR_DEFAULT_EXCHANGE = "conductor.topic";
    protected LinkedBlockingQueue<Message> messages = new LinkedBlockingQueue<>();
    private String queueName;
    private  int pollTimeInMS;
    private  int longPollTimeout;
    private  int pollCount;
    private String exchange;
    private Channel channel;

    public RabbitObservableQueue(String queueNameWithExchange, Configuration configuration) throws IOException, TimeoutException {
    	//Handle queueName which will be of format exchangeName:queueName
    	int index = queueNameWithExchange.indexOf(':');
		if (index == -1) {
			logger.error("Queue cannot be configured without exchange name: {}", queueNameWithExchange);
			throw new IllegalArgumentException("Illegal queue name " + queueNameWithExchange);
		}
		this.exchange = queueNameWithExchange.substring(0, index);
		this.queueName = queueNameWithExchange.substring(index + 1);
        this.pollTimeInMS = configuration.getIntProperty("rabbit.queue.pollingInterval", RABBIT_QUEUE_DEFAULTPOLLINGINTERVAL);
        this.longPollTimeout = configuration.getIntProperty("rabbit.queue.longPollTimeout", RABBIT_QUEUE_DEFAULTLONGPOLLTIMEOUT);
        this.pollCount = configuration.getIntProperty("rabbit.queue.pollCount", RABBIT_QUEUE_DEFAULTPOLLCOUNT);
        channel = configureChannel(configuration);
        channel.exchangeDeclare(this.exchange, BuiltinExchangeType.TOPIC, true);
        logger.info("RabbitObservableQueue initialized for {}", this.queueName);
    }

    public RabbitObservableQueue(String queueNameWithExchange){
        int index = queueNameWithExchange.indexOf(':');
        if (index == -1) {
            logger.error("Queue cannot be configured without exchange name: {}", queueNameWithExchange);
            throw new IllegalArgumentException("Illegal queue name " + queueNameWithExchange);
        }
        this.exchange = queueNameWithExchange.substring(0, index);
        this.queueName = queueNameWithExchange.substring(index + 1);
    }

    private Channel configureChannel(Configuration configuration) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUsername(configuration.getProperty("rabbit.Username",""));
        connectionFactory.setPassword(configuration.getProperty("rabbit.Password",""));
        connectionFactory.setHost(configuration.getProperty("rabbit.Host",""));
        connectionFactory.setPort(configuration.getIntProperty("rabbit.Port",RABBIT_PORT_DEFAULTPORT));
        Connection connection = connectionFactory.newConnection();
        return connection.createChannel();
    }

    @Override
    public Observable<Message> observe() {
        Observable.OnSubscribe<Message> subscriber = getOnSubscribe();
        return Observable.create(subscriber);
    }

    @Override
    public String getType() {
        return QUEUE_TYPE;
    }

    @Override
    public String getName() {
        return queueName;
    }

    @Override
    public String getURI() {
        return queueName;
    }

    @Override
    public List<String> ack(List<Message> messages) {
        return null;
    }

    @Override
    public void publish(List<Message> messages) {
        try {
            publishMessage(messages);
        } catch (IOException e) {
            throw new ApplicationException(ApplicationException.Code.INVALID_INPUT, "Failed To Publish To The Queue");
        } catch (TimeoutException e) {
            throw new ApplicationException(ApplicationException.Code.NOT_FOUND, "Failed To Publish To The Queue");
        }
    }

    @Override
    public void setUnackTimeout(Message message, long unackTimeout) {
    }

    @Override
    public long size() {
        return 0;
    }

    @VisibleForTesting
    public void publishMessage(List<Message> messages) throws IOException, TimeoutException {
        logger.info("Sending {} messages", messages.size());
        messages.forEach(msg -> {

            if (channel != null) {
                try {
                    channel.exchangeDeclare(this.exchange, BuiltinExchangeType.TOPIC, true);
                    AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder();
                    Map<String,Object> headers = new HashMap<>();
                    headers.put("receipt",msg.getReceipt());
                    builder.contentType("text/plain");
                    builder.messageId(msg.getId());
                    builder.headers(headers);
                    channel.basicPublish(this.exchange, queueName, builder.build(), msg.getPayload().getBytes());
                    logger.info("Sending {} messages", messages.size());
                } catch (IOException e) {
                    throw new ApplicationException(ApplicationException.Code.INVALID_INPUT, "Failed To Publish To The Queue");
                }
            }
        });
    }
    
    @VisibleForTesting
    public void receiveMessages() {
        try {

            channel.exchangeDeclare(this.exchange, BuiltinExchangeType.TOPIC, true);
            Message message = new Message();
            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
                        throws IOException {
                    String response = new String(body, "UTF-8");
                    if (response != null) {
                        message.setId(NUID.nextGlobal());
                        message.setPayload(response);
                        message.setReceipt("rec");
                        messages.add(message);
                        logger.info("Message not received from IMS {}", message.getPayload());
                    }
                }
            };
            channel.basicConsume(queueName, true, consumer);
            Monitors.recordEventQueueMessagesProcessed(QUEUE_TYPE, this.queueName, messages.size());
        } catch (Exception e) {
            logger.error("Exception while getting messages from Rabbit ", e);
            Monitors.recordObservableQMessageReceivedErrors(QUEUE_TYPE);
        }
    }

    @VisibleForTesting
    public Observable.OnSubscribe<Message> getOnSubscribe() {
        return subscriber -> {
            Observable<Long> interval = Observable.interval(pollTimeInMS, TimeUnit.MILLISECONDS);
            interval.flatMap((Long x)->{
                receiveMessages();
                List<Message> available = new LinkedList<>();
                messages.drainTo(available);
                if (!available.isEmpty()) {
                    AtomicInteger count = new AtomicInteger(0);
                    StringBuilder buffer = new StringBuilder();
                    available.forEach(msg -> {
                        buffer.append(msg.getId()).append("=").append(msg.getPayload());
                        count.incrementAndGet();
                        if (count.get() < available.size()) {
                            buffer.append(",");
                        }
                    });

                    logger.info(String.format("Batch from %s to conductor is %s", queueName, buffer.toString()));
                }
                return Observable.from(available);
            }).subscribe(subscriber::onNext, subscriber::onError);
        };
    }

}
