package com.rabbitmq.pubsub.direct.routing;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import com.rabbitmq.client.Channel;

public class EmitLogDirect {

	private static final String EXCHANGE_NAME = "direct_logs";
	private static final String EXCHANGE_TYPE = "direct";
	private static final String[] LOG_ERROR = {"ERROR", "Error log message"};
	private static final String[] LOG_WARNING = {"WARNING", "warning log message"};
	private static final String[] LOG_INFO = {"INFO", "info log message"};

	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
			channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);

			sendLog(channel, LOG_ERROR);
			sendLog(channel, LOG_WARNING);
			sendLog(channel, LOG_INFO);
		}
	}

	private static void sendLog(Channel channel, String[] log) throws IOException, UnsupportedEncodingException {
		String routingKey = log[0];
		String message = log[1];
		channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
		System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");
	}
}
