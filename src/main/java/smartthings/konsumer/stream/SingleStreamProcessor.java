package smartthings.konsumer.stream;

import smartthings.konsumer.MessageProcessor;
import kafka.consumer.KafkaStream;

public class SingleStreamProcessor implements StreamProcessor {
	private static final int DEFAULT_TRY_COUNT = 1;
	private final int tryCount;

	public SingleStreamProcessor() {
		this(DEFAULT_TRY_COUNT);
	}

	public SingleStreamProcessor(int tryCount) {
		this.tryCount = tryCount;
	}

	@Override
	public Runnable buildConsumer(KafkaStream<byte[], byte[]> stream, MessageProcessor processor) {
		return new SingleMessageConsumer(stream, processor, tryCount);
	}

	@Override
	public void shutdown() {
		// Nothing to do here. Shutting down the partition thread pool should be enough.
	}
}
