package com.charlieknudsen.konsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageEnvelope implements Runnable {

	private final static Logger log = LoggerFactory.getLogger(MessageEnvelope.class);
	private final ExceptionHandler exceptionHandler;
	private final MessageProcessor processor;
	private final byte[] bytes;
	private int tryCount;

	public MessageEnvelope(MessageProcessor processor, ExceptionHandler exceptionHandler, byte[] bytes) {
		this.tryCount = 1;
		this.bytes = bytes;
		this.processor = processor;
		this.exceptionHandler = exceptionHandler;
	}

	@Override
	public void run() {
		try {
			tryCount++;
			processor.processMessage(bytes);
		} catch (Exception e) {
			exceptionHandler.handleException(this, e);
		}
	}

	public int getTryCount() {
		return tryCount;
	}
}
