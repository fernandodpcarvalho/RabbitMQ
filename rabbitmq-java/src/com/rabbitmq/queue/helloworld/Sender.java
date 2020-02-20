package com.rabbitmq.queue.helloworld;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;

import java.util.Map;

import com.rabbitmq.client.Channel;

public class Sender {

	private static final String EXCHANGE_NAME = ""; //Default exchange: Direct with no name.
	private static final String QUEUE_NAME = "hello";
	private static final boolean DURABLE = true;
	private static final boolean EXCLUSIVE = false;
	private static final boolean AUTODELETE = false;
	private static final Map<String, Object> ARGUMENTS = null;
	private static final String MESSAGE = "Hello World!";

	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
			channel.queueDeclare(QUEUE_NAME, DURABLE, EXCLUSIVE, AUTODELETE, ARGUMENTS);
			channel.basicPublish(EXCHANGE_NAME, QUEUE_NAME, null, MESSAGE.getBytes());
			System.out.println(" [x] Sent '" + MESSAGE + "'");
		}
	}
}
