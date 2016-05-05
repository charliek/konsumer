package smartthings.konsumer.stream;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smartthings.konsumer.circuitbreaker.CircuitBreaker;
import smartthings.konsumer.filterchain.MessageFilterChain;

public class SingleMessageConsumer implements Runnable {

	private final static Logger log = LoggerFactory.getLogger(SingleMessageConsumer.class);

	private final KafkaStream<byte[], byte[]> stream;
	private final MessageFilterChain filterChain;
	private final CircuitBreaker circuitBreaker;

	public SingleMessageConsumer(KafkaStream<byte[], byte[]> stream, MessageFilterChain filterChain,
								 CircuitBreaker circuitBreaker) {
		this.stream = stream;
		this.filterChain = filterChain;
		this.circuitBreaker = circuitBreaker;
	}

	@Override
	public void run() {
		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext()) {
			circuitBreaker.blockIfOpen();
			MessageAndMetadata<byte[], byte[]> messageAndMetadata = it.next();
			processMessage(messageAndMetadata);
		}
		log.warn("Shutting down listening thread");
	}

	private void processMessage(MessageAndMetadata<byte[], byte[]> messageAndMetadata) {
		try {
			filterChain.handle(messageAndMetadata);
			return;
		} catch (Exception e) {
			log.error("Exception occurred during message processing", e);
		}
		log.warn("Shutting down listening thread");
	}
}
