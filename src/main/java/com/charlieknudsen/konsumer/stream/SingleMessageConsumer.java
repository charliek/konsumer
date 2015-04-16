package com.charlieknudsen.konsumer.stream;

import com.charlieknudsen.konsumer.MessageProcessor;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleMessageConsumer implements Runnable {

	private final static Logger log = LoggerFactory.getLogger(SingleMessageConsumer.class);

	private final KafkaStream<byte[], byte[]> stream;
	private final MessageProcessor processor;

	public SingleMessageConsumer(KafkaStream<byte[], byte[]> stream, MessageProcessor processor) {
		this.stream = stream;
		this.processor = processor;
	}

	@Override
	public void run() {
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext()) {
			MessageAndMetadata<byte[], byte[]> messageAndMetadata = it.next();
			try {
				processor.processMessage(messageAndMetadata);
			} catch (Exception e) {
				// TODO currently not handling retries at all
				log.error("Exception occurred during message processing.", e);
			}
		}
		log.warn("Shutting down listening thread");
	}
}
