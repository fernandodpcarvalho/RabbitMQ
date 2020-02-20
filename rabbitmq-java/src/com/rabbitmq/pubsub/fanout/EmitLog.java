package com.rabbitmq.pubsub.fanout;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

//When a fanout exchange receives a message, a copy of this message is sent to all queues bound to it.
//Unlike the Direct type, in the fanout, we don't have the Routing Key figure.

public class EmitLog {

	private static final String EXCHANGE_NAME = "logs";
	private static final String EXCHANGE_TYPE = "fanout";
	private static final String ROUTING_KEY = "";
	public static final String MESSAGE = "error";

	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
			channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);
			channel.basicPublish(EXCHANGE_NAME, ROUTING_KEY, null, MESSAGE.getBytes("UTF-8"));
			System.out.println(" [x] Sent '" + MESSAGE + "'");
		}
	}
}
