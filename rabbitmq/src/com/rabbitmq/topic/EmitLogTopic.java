package com.rabbitmq.topic;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import com.rabbitmq.client.Channel;

//Topic: Similar to Direct, but the Routing Keys can use patters
//Routing keys of logs will have two words: "<facility>.<severity>".

public class EmitLogTopic {

	private static final String EXCHANGE_NAME = "topic_logs";
	private static final String EXCHANGE_TYPE = "topic";

	private static final String[] LOG_KERNEL_CRITICAL = {"kernel.critical", "A critical kernel error"};
	private static final String[] LOG_KERNEL_INFO = {"kernel.info", "A info kernel error"};
	private static final String[] LOG_MEMORY_WARNING = {"memory.warning", "A warning memory error"};
	private static final String[] LOG_APP_INFO = {"app.info", "A info app error"};

	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
			channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);

			sendLog(channel, LOG_KERNEL_CRITICAL);
			sendLog(channel, LOG_KERNEL_INFO);
			sendLog(channel, LOG_MEMORY_WARNING);
			sendLog(channel, LOG_APP_INFO);
		}
	}
	
	private static void sendLog(Channel channel, String[] log) throws IOException, UnsupportedEncodingException {
		String routingKey = log[0];
		String message = log[1];
		channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
		System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");
	}
}
