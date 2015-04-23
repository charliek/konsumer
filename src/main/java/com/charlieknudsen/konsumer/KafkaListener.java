package com.charlieknudsen.konsumer;

import com.charlieknudsen.konsumer.stream.StreamProcessor;
import com.charlieknudsen.konsumer.stream.StreamProcessorFactory;
import com.charlieknudsen.konsumer.util.QuietCallable;
import com.charlieknudsen.konsumer.util.RunUtil;
import com.charlieknudsen.konsumer.util.ThreadFactoryBuilder;
import kafka.consumer.Consumer;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class KafkaListener {
	private final static Logger log = LoggerFactory.getLogger(KafkaListener.class);

	private final ConsumerConnector consumer;
	private final ExecutorService partitionExecutor;
	private final StreamProcessor streamProcessor;
	private final String topic;
	private final ListenerConfig config;

	public KafkaListener(ListenerConfig config) {
		this.config = config;
		// Build custom executor so we control the factory and backing queue
		// and get better thread names for logging
		partitionExecutor = buildPartitionExecutor();

		consumer = Consumer.createJavaConsumerConnector(config.getConsumerConfig());
		topic = config.getTopic();
		streamProcessor = new StreamProcessorFactory(config).getProcessor();
	}

	private ExecutorService buildPartitionExecutor() {
		ThreadFactory threadFactory = new ThreadFactoryBuilder()
				.setNameFormat("KafkaPartition-" + config.getTopic() + "-%d")
				.setDaemon(config.useDaemonThreads())
				.build();
		return Executors.newFixedThreadPool(config.getPartitionThreads(), threadFactory);
	}

	public void shutdown() {
		consumer.shutdown();
		partitionExecutor.shutdown();
		try {
			boolean completed = partitionExecutor.awaitTermination(config.getShutdownAwaitSeconds(), TimeUnit.SECONDS);
			if (completed) {
				log.info("Shutdown partition consumers of topic {} all messages processed", topic);
			} else {
				log.warn("Shutdown partition consumers of topic {}. Some messages left unprocessed.", topic);
			}
		} catch (InterruptedException e) {
			log.error("Interrupted while waiting for shutdown of topic {}", topic, e);
		}
		streamProcessor.shutdown();
	}

	public void run(MessageProcessor processor) {
		Map<String, Integer> topicCountMap = new HashMap<>();
		topicCountMap.put(topic, config.getPartitionThreads());
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

		log.info("Listening to kafka with {} partition threads", config.getPartitionThreads());
		for (KafkaStream<byte[], byte[]> stream : streams) {
			try {
				partitionExecutor.submit(streamProcessor.buildConsumer(stream, processor));
			} catch (RejectedExecutionException e) {
				log.error("Error submitting job to partition executor");
				throw e;
			}

		}
	}

	/**
	 * Run and then block the calling thread until shutdown. In place to make it easy to
	 * consume in a main method and still exit cleanly.
	 */
	public void runAndBlock(MessageProcessor processor) {
		run(processor);
		RunUtil.blockForShutdown(new QuietCallable() {
			@Override
			public void call() {
				shutdown();
			}
		});
	}
}
