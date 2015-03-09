package com.charlieknudsen.konsumer.example;

import com.charlieknudsen.konsumer.ListenerConfig;
import com.charlieknudsen.konsumer.KafkaListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;

public class Main {

	private final static Logger log = LoggerFactory.getLogger(Main.class);

	private void blockForShutdown(final Callable<Void> onShutdown) throws Exception {
		final ArrayBlockingQueue<Object> shutdownQueue = new ArrayBlockingQueue<>(1);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				log.warn("Beginning to shutdown kafka");
				try {
					onShutdown.call();
					log.warn("Shutdown complete.");
				} catch (Exception e) {
					log.error("Error caught during shutdown", e);
				}
				shutdownQueue.add(new Object());
			}
		});
		// This feels a bit janky but avoids a busy wait and seems to work
		shutdownQueue.take();
	}

	private void run() throws Exception {
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
		consumer.run(new LoggingMessageProcessor());

		// Don't let the main thread exit or the application will exit
		// and stop processing messages
		blockForShutdown(new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				consumer.shutdown();
				return null;
			}
		});
	}

	public static void main(String[] args) throws Exception {
		new Main().run();
	}

}
