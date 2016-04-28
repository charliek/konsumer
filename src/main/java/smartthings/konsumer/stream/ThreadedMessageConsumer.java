package smartthings.konsumer.stream;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smartthings.konsumer.ListenerConfig;
import smartthings.konsumer.circuitbreaker.CircuitBreaker;
import smartthings.konsumer.filterchain.MessageFilterChain;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;

public class ThreadedMessageConsumer implements Runnable {
	private final static Logger log = LoggerFactory.getLogger(ThreadedMessageConsumer.class);

	/**
	 * The kafka stream we will be pulling messages of of.
	 */
	private final KafkaStream<byte[], byte[]> stream;

	/**
	 * The executor for message processing.
	 */
	private final Executor messageExecutor;

	/**
	 * Configuration of this listener.
	 */
	private final ListenerConfig config;

	/**
	 * Chain of filters followed by the message processor.
	 */
	private final MessageFilterChain filterChain;

	/**
	 * Semaphone used so we don't consumer messages faster than we can process them.
	 */
	private final Semaphore taskSemaphone;

	/**
	 * Circuit breaker to stop processing of messages.
	 */
	private final CircuitBreaker circuitBreaker;

	public ThreadedMessageConsumer(
			KafkaStream<byte[], byte[]> stream, Executor messageExecutor, ListenerConfig config,
			MessageFilterChain filterChain, CircuitBreaker circuitBreaker
	) {
		this.stream = stream;
		this.messageExecutor = messageExecutor;
		this.config = config;
		this.filterChain = filterChain;
		this.taskSemaphone = new Semaphore(config.getProcessingThreads());
		this.circuitBreaker = circuitBreaker;
	}

	private void submitTask(final MessageAndMetadata<byte[], byte[]> messageAndMetadata) throws InterruptedException {
		try {
			taskSemaphone.acquire();
		} catch (InterruptedException e) {
			log.warn("Interrupted while trying to submit task to consumer.", e);
			throw e;
		}
		try {
			messageExecutor.execute(new Runnable() {
				@Override
				public void run() {
					try {
						filterChain.handle(messageAndMetadata);
					} catch (Exception e) {
						handleException(messageAndMetadata, e);
					} finally {
						taskSemaphone.release();
					}
				}
			});
		} catch (RejectedExecutionException e) {
			log.error("Error submitting consumer task", e);
			throw e;
		}
	}

	@Override
	public void run() {
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext()) {
			circuitBreaker.blockIfOpen();
			MessageAndMetadata<byte[], byte[]> messageAndMetadata = it.next();
			try {
				submitTask(messageAndMetadata);
			} catch (Exception e) {
				log.error("Unexpected exception occurred during message processing. Exiting.", e);
				break;
			}
		}
		log.warn("Shutting down listening thread");
	}

	public void handleException(MessageAndMetadata<byte[], byte[]> messageAndMetadata, Throwable t) {
		log.warn("Exception when processing message", t);
	}
}
