package com.rabbitmq.queue.workqueue;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.Connection;

import java.util.Map;

import com.rabbitmq.client.Channel;

public class NewTask {

	private static final String EXCHANGE_NAME = ""; //Default exchange: Direct with no name.
	private static final String TASK_QUEUE_NAME = "workqueue";
	private static final boolean DURABLE = true;
	private static final boolean EXCLUSIVE = false;
	private static final boolean AUTODELETE = false;
	private static final Map<String, Object> ARGUMENTS = null;
	
	public static String MESSAGE = "test.firstTask";

	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
			channel.queueDeclare (TASK_QUEUE_NAME, DURABLE, EXCLUSIVE, AUTODELETE, ARGUMENTS);
			channel.basicPublish(EXCHANGE_NAME, TASK_QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, MESSAGE.getBytes());
			System.out.println(" [x] Sent '" + MESSAGE + "'");

		}
	}
}
