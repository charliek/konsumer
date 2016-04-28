package smartthings.konsumer.stream;

import kafka.consumer.KafkaStream;
import smartthings.konsumer.circuitbreaker.CircuitBreaker;
import smartthings.konsumer.filterchain.MessageFilterChain;

public interface StreamProcessor {

	/**
	 * Build a consumer for a kafka stream
	 * @param stream A kafka stream that could be 0..N partitions
	 * @param filterChain The filterChain responsible for executing all filters in the correct order
	 * @param circuitBreaker The circuitBreaker that is responsible for stopping the consumer if the circuit is opened.
	 * @return a runnable that will be executed in a thread pool
	 */
	Runnable buildConsumer(KafkaStream<byte[], byte[]> stream, MessageFilterChain filterChain, CircuitBreaker circuitBreaker);

	/**
	 * This will be called on shutdown to do any clean up that may be necessary
	 */
	void shutdown();
}
