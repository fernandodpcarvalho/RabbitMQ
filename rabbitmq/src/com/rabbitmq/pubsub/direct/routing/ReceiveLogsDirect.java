package com.rabbitmq.pubsub.direct.routing;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class ReceiveLogsDirect {
	private static final String EXCHANGE_NAME = "direct_logs";
	private static final String EXCHANGE_TYPE = "direct";
	private static final String BINDING_KEY = "black";

	private static final String ROUTING_KEY_ERROR = "ERROR";
	private static final String ROUTING_KEY_WARNING = "WARNING";
	private static final String ROUTING_KEY_INFO = "INFO";

	private static final String[] ROUTING_KEY = {ROUTING_KEY_ERROR, ROUTING_KEY_WARNING};
//	private static final String[] ROUTING_KEY = {ROUTING_KEY_ERROR};

	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);

		String queueName = channel.queueDeclare().getQueue();

		if (ROUTING_KEY.length < 1) {
			System.err.println("Usage: ReceiveLogsDirect [info] [warning] [error]");
			System.exit(1);
		}
		
		StringBuilder routingKeys = new StringBuilder(0);

		for (String ROUTING_KEY : ROUTING_KEY) {
			routingKeys.append("["+ROUTING_KEY+"] ");
			channel.queueBind(queueName, EXCHANGE_NAME, ROUTING_KEY); //Severity of log message
		}

		channel.queueBind(queueName, EXCHANGE_NAME, BINDING_KEY);

		System.out.println(" [*] Waiting for messages of severity: "+routingKeys+". To exit press CTRL+C");

		DeliverCallback deliverCallback = (consumerTag, delivery) -> {
			String message = new String(delivery.getBody(), "UTF-8");
			System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
		};

		channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
		});
	}
}