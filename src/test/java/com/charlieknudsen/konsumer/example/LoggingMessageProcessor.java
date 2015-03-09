package com.charlieknudsen.konsumer.example;

import com.charlieknudsen.konsumer.MessageProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingMessageProcessor implements MessageProcessor {
	private final static Logger log = LoggerFactory.getLogger(LoggingMessageProcessor.class);

	@Override
	public void processMessage(byte[] bytes) throws Exception {
		log.warn("Thread {} - Got message - {}", Thread.currentThread().getName(), new String(bytes));
	}
}
