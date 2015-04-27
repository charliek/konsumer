package smartthings.konsumer.stream;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smartthings.konsumer.ExceptionHandler;
import smartthings.konsumer.ListenerConfig;
import smartthings.konsumer.MessageEnvelope;
import smartthings.konsumer.MessageProcessor;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;

public class ThreadedMessageConsumer implements Runnable, ExceptionHandler {
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
	 * The class that will actually to do the work of processing the messages.
	 */
	private final MessageProcessor processor;

	/**
	 * Semaphone used so we don't consumer messages faster than we can process them.
	 */
	private final Semaphore taskSemaphone;

	public ThreadedMessageConsumer(
			KafkaStream<byte[], byte[]> stream, Executor messageExecutor,
			ListenerConfig config, MessageProcessor processor
	) {
		this.stream = stream;
		this.messageExecutor = messageExecutor;
		this.config = config;
		this.processor = decorateProcessor(processor);
		this.taskSemaphone = new Semaphore(config.getProcessingThreads());
	}

	private MessageProcessor decorateProcessor(final MessageProcessor processor) {
		return new MessageProcessor() {
			@Override
			public void processMessage(MessageAndMetadata<byte[], byte[]> message) throws Exception {
				try {
					processor.processMessage(message);
				} finally {
					taskSemaphone.release();
				}
			}
		};
	}

	private void submitTask(MessageEnvelope envelope) throws InterruptedException {
		try {
			taskSemaphone.acquire();
		} catch (InterruptedException e) {
			log.warn("Interrupted while trying to submit task to consumer.", e);
			throw e;
		}
		try {
			messageExecutor.execute(envelope);
		} catch (RejectedExecutionException e) {
			log.error("Error submitting consumer task", e);
			throw e;
		}
	}

	@Override
	public void run() {
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext()) {
			MessageAndMetadata<byte[], byte[]> messageAndMetadata = it.next();
			try {
				submitTask(new MessageEnvelope(processor, this, messageAndMetadata));
			} catch (Exception e) {
				log.error("Unexpected exception occurred during message processing. Exiting.", e);
				break;
			}
		}
		log.warn("Shutting down listening thread");
	}

	@Override
	public void handleException(MessageEnvelope msg, Throwable t) {
		log.warn("Exception when processing message", t);
		if (msg.getTryCount() <= config.getTryCount()) {
			try {
				submitTask(msg);
			} catch (Exception e) {
				log.error("Unexpected exception occurred trying to re-queue a failed message. Aborting retry.", e);
			}
		}
	}
}
