package smartthings.konsumer.stream;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smartthings.konsumer.MessageProcessor;

public class SingleMessageConsumer implements Runnable {

	private final static Logger log = LoggerFactory.getLogger(SingleMessageConsumer.class);

	private final KafkaStream<byte[], byte[]> stream;
	private final MessageProcessor processor;
	private final int tryCount;

	public SingleMessageConsumer(KafkaStream<byte[], byte[]> stream, MessageProcessor processor, int tryCount) {
		this.stream = stream;
		this.processor = processor;
		if (tryCount < 1) {
			// Try count must be at least one
			tryCount = 1;
		}
		this.tryCount = tryCount;
	}

	@Override
	public void run() {
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext()) {
			MessageAndMetadata<byte[], byte[]> messageAndMetadata = it.next();
			processMessage(messageAndMetadata);
		}
		log.warn("Shutting down listening thread");
	}

	private void processMessage(MessageAndMetadata<byte[], byte[]> messageAndMetadata) {
		for (int cnt = 1; cnt <= tryCount; cnt++) {
			try {
				processor.processMessage(messageAndMetadata);
				return;
			} catch (Exception e) {
				if (cnt == 1) {
					log.error("Exception occurred during message processing.", e);
				} else {
					log.error("Exception occurred during message processing. Try {}.", cnt, e);
				}
			}
		}
		log.warn("Shutting down listening thread");
	}
}
