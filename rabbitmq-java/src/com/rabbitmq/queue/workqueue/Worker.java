package com.rabbitmq.queue.workqueue;

import java.util.Map;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

//This queue will response with an ACT when the task is finished
//By default, once RabbitMQ delivers a message to the consumer it immediately marks it for deletion. 
//So if the worker dies, the message will be lost.
//But it's possible to configure RabbitMQ to wait for a ack before delete the message (autoAck). This way the message is won't be lost.
//If RabbitMQ doesn't receive the ack, it'll understand that a message wasn't processed fully and will re-queue it.

public class Worker {

	private static final String TASK_QUEUE_NAME = "workqueue";
	private static final boolean DURABLE = true;
	private static final boolean EXCLUSIVE = false;
	private static final boolean AUTODELETE = false;
	private static final Map<String, Object> ARGUMENTS = null;
	private static final int PREFETCHCOUNT = 1; //maximum number of messages that the server will deliver, 0 if unlimited

	private static final boolean AUTO_ACK = false;
	
	
	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost("localhost");
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		channel.queueDeclare(TASK_QUEUE_NAME, DURABLE, EXCLUSIVE, AUTODELETE, ARGUMENTS);
		channel.basicQos(PREFETCHCOUNT); 
		System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

		DeliverCallback deliverCallback = (consumerTag, delivery) -> {
			String message = new String(delivery.getBody(), "UTF-8");

			System.out.println(" [x] Received '" + message + "'");
			
			try {
				doWork(message);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				System.out.println(" [x] Done");
			    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
			}
		};
		channel.basicConsume(TASK_QUEUE_NAME, AUTO_ACK, deliverCallback, consumerTag -> {
		});
	}

	private static void doWork(String task) throws InterruptedException {
		for (char ch : task.toCharArray()) {
			if (ch == '.')
				Thread.sleep(10000);
		}
	}
}
