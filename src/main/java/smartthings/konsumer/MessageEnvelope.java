package smartthings.konsumer;

import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageEnvelope implements Runnable {

	private final static Logger log = LoggerFactory.getLogger(MessageEnvelope.class);
	private final ExceptionHandler exceptionHandler;
	private final MessageProcessor processor;
	private final MessageAndMetadata<byte[], byte[]> message;
	private int tryCount;

	public MessageEnvelope(MessageProcessor processor, ExceptionHandler exceptionHandler, MessageAndMetadata<byte[], byte[]> message) {
		this.tryCount = 1;
		this.message = message;
		this.processor = processor;
		this.exceptionHandler = exceptionHandler;
	}

	@Override
	public void run() {
		try {
			tryCount++;
			processor.processMessage(message);
		} catch (Exception e) {
			exceptionHandler.handleException(this, e);
		}
	}

	public int getTryCount() {
		return tryCount;
	}
}
