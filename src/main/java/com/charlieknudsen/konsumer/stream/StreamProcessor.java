package com.charlieknudsen.konsumer.stream;

import com.charlieknudsen.konsumer.MessageProcessor;
import kafka.consumer.KafkaStream;

public interface StreamProcessor {

	/**
	 * Build a consumer for a kafka stream
	 * @param stream A kafka stream that could be 0..N partitions
	 * @param processor The class to use to process messages
	 * @return a runnable that will be executed in a thread pool
	 */
	Runnable buildConsumer(KafkaStream<byte[], byte[]> stream, MessageProcessor processor);

	/**
	 * This will be called on shutdown to do any clean up that may be necessary
	 */
	void shutdown();
}
