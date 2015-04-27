package smartthings.konsumer;

import kafka.message.MessageAndMetadata;

public interface MessageProcessor {
	void processMessage(MessageAndMetadata<byte[], byte[]> message) throws Exception;
}
