package smartthings.konsumer.filterchain;

import kafka.message.MessageAndMetadata;

public interface MessageFilter {

	void handleMessage(MessageAndMetadata<byte[], byte[]> messageAndMetadata, MessageContext ctx) throws Exception;

}
