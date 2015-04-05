package com.charlieknudsen.konsumer.example

import com.charlieknudsen.konsumer.KafkaListener
import com.charlieknudsen.konsumer.ListenerConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory

public class Main {

	private final static Logger log = LoggerFactory.getLogger(Main.class);

	private void run() throws Exception {
		log.info("Lets listen to Kafka!!!")
		ListenerConfig config = new ListenerConfig.Builder()
				.partitionThreads(1)
				.processingThreads(8)
				.processingQueueSize(10)
				.tryCount(2)
				.consumerGroup("konsumer-test")
				.topic("logs")
				.zookeeper("127.0.0.1:2181")
				.setProperty("auto.offset.reset", "smallest")
				.build();
		final KafkaListener consumer = new KafkaListener(config);
		// call the blocking run method so the application doesn't exit and
		// stop the queue processing
		consumer.runAndBlock(new LoggingMessageProcessor());
	}

	public static void main(String[] args) throws Exception {
		new Main().run();
	}
}
