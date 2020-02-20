package com.rabbitmq.topic;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

//Topic: Similar to Direct, but the Routing Keys can use patters
//Routing keys of logs will have two words: "<facility>.<severity>".

public class ReceiveLogsTopic {
	private static final String EXCHANGE_NAME = "topic_logs";
	private static final String EXCHANGE_TYPE = "topic";

	private static final String[] ROUTING_KEY_PATTERN = {"kernel.*", "*.critical", "*.warning"};
//	private static final String[] ROUTING_KEY_PATTERN = {"*.critical"};
	
	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);

		String queueName = channel.queueDeclare().getQueue();

		if (ROUTING_KEY_PATTERN.length < 1) {
			System.err.println("Must especify the Routing Key Pattern: <>.<>");
			System.exit(1);
		}

		StringBuilder bindingKeys = new StringBuilder();
		
		for (String bindingKey : ROUTING_KEY_PATTERN) {
			bindingKeys.append(bindingKey+" ");
			channel.queueBind(queueName, EXCHANGE_NAME, bindingKey);
		}

		System.out.println(" [*] Waiting for messages of kind: '"+bindingKeys+"'. To exit press CTRL+C");

		DeliverCallback deliverCallback = (consumerTag, delivery) -> {
			String message = new String(delivery.getBody(), "UTF-8");
			System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
		};
		channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
		});
	}
}