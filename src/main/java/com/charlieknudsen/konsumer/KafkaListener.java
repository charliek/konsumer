package com.charlieknudsen.konsumer;

import com.charlieknudsen.konsumer.util.QuietCallable;
import com.charlieknudsen.konsumer.util.RunUtil;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
	private final ExecutorService processingExecutor;
	private final String topic;
	private final ListenerConfig config;

	public KafkaListener(ListenerConfig config) {
		this.config = config;
		// Build custom executor so we control the factory and backing queue
		// and get better thread names for logging
		partitionExecutor = buildPartitionExecutor();
		processingExecutor = buildConsumerExecutor();
		consumer = Consumer.createJavaConsumerConnector(config.getConsumerConfig());
		topic = config.getTopic();
	}

	private ExecutorService buildPartitionExecutor() {
		ThreadFactory threadFactory = new ThreadFactoryBuilder()
				.setNameFormat("KafkaPartition-" + config.getTopic() + "-%d")
				.setDaemon(config.useDaemonThreads())
				.build();
		return Executors.newFixedThreadPool(config.getPartitionThreads(), threadFactory);
	}

	private ExecutorService buildConsumerExecutor() {
		ThreadFactory messageThreadFactory = new ThreadFactoryBuilder()
				.setNameFormat("KafkaConsumer-" + config.getTopic() + "-%d")
				.setDaemon(config.useDaemonThreads())
				.build();
		return new ThreadPoolExecutor(
			config.getProcessingThreads(),
			config.getProcessingThreads(),
			0L,
			TimeUnit.MILLISECONDS,
			new LinkedBlockingQueue<Runnable>(config.getProcessingQueueSize()),
			messageThreadFactory
		);
	}

	public void shutdown() {
		consumer.shutdown();
		partitionExecutor.shutdown();
		processingExecutor.shutdown();
		try {
			boolean completed = processingExecutor.awaitTermination(4, TimeUnit.SECONDS);
			if (completed) {
				log.info("Shutdown consumers of topic {} all messages processed", topic);
			} else {
				log.warn("Shutdown consumers of topic {}. Some messages left unprocessed.", topic);
			}
		} catch (InterruptedException e) {
			log.error("Interrupted while waiting for shutdown of topic {}", topic, e);
		}
	}

	public void run(MessageProcessor processor) {
		Map<String, Integer> topicCountMap = new HashMap<>();
		topicCountMap.put(topic, config.getPartitionThreads());
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

		for (KafkaStream stream : streams) {
			partitionExecutor.submit(new MessageConsumer(stream, processingExecutor, config, processor));
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
