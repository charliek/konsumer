package com.charlieknudsen.konsumer;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;

public class MessageConsumer implements Runnable, ExceptionHandler {
	private final static Logger log = LoggerFactory.getLogger(MessageConsumer.class);

	/**
	 * The kafka stream we will be pulling messages of of.
	 */
	private final KafkaStream stream;

	/**
	 * The executor for message processing.
	 */
	private final ExecutorService messageExecutor;

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

	public MessageConsumer(
			KafkaStream stream, ExecutorService messageExecutor,
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
			public void processMessage(byte[] bytes) throws Exception {
				try {
					processor.processMessage(bytes);
				} finally {
					taskSemaphone.release();
				}
			}
		};
	}

	private void submitTask(MessageEnvelope envelope) throws InterruptedException {
		try {
			taskSemaphone.acquire();
			messageExecutor.submit(envelope);
		} catch (RejectedExecutionException|InterruptedException e) {
			log.error("Error submitting consumer task", e);
			throw e;
		}
	}

	public void run() {
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext()) {
			byte[] bytes = it.next().message();
			try {
				submitTask(new MessageEnvelope(processor, this, bytes));
			} catch (Exception e) {
				log.error("Unexpected exception occurred during message processing. Exiting.", e);
				break;
			}
		}
		log.debug("Shutting down listening thread");
	}

	@Override
	public void handleException(MessageEnvelope msg, Throwable t) {
		log.warn("Exception when processing message", t);
		if (msg.getTryCount() < config.getTryCount()) {
			try {
				submitTask(msg);
			} catch (Exception e) {
				log.error("Unexpected exception occurred trying to re-queue a failed message. Aborting retry.", e);
			}
		}
	}
}
