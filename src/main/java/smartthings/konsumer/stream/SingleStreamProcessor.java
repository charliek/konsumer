package smartthings.konsumer.stream;

import smartthings.konsumer.MessageProcessor;
import kafka.consumer.KafkaStream;

public class SingleStreamProcessor implements StreamProcessor {

	public SingleStreamProcessor() {
	}

	@Override
	public Runnable buildConsumer(KafkaStream<byte[], byte[]> stream, MessageProcessor processor) {
		return new SingleMessageConsumer(stream, processor);
	}

	@Override
	public void shutdown() {
		// Nothing to do here. Shutting down the partition thread pool should be enough.
	}
}
