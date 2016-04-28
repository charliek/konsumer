package smartthings.konsumer.filters;

import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smartthings.konsumer.filterchain.MessageContext;
import smartthings.konsumer.filterchain.MessageFilter;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class RetryMessageFilter implements MessageFilter {

	private final static Logger log = LoggerFactory.getLogger(RetryMessageFilter.class);

	private final int retryCount;
	private final boolean swallowErrorAfterRetries;

	public RetryMessageFilter(int retryCount) {
		this(retryCount, false);
	}

	public RetryMessageFilter(int retryCount, boolean swallowErrorAfterRetries) {
		assert retryCount > 0;
		this.retryCount = retryCount;
		this.swallowErrorAfterRetries = swallowErrorAfterRetries;
	}

	@Override
	public void handleMessage(MessageAndMetadata<byte[], byte[]> messageAndMetadata, MessageContext ctx) throws Exception {
		Exception latestException = null;
		for (int i = 0; i < retryCount; i++) {
			try {
				ctx.next(messageAndMetadata);
				return;
			} catch (Exception e) {
				latestException = e;
				log.warn("Message {} from partition {} failed to process.", messageAndMetadata.offset(), messageAndMetadata.partition(), e);
			}
		}
		if (swallowErrorAfterRetries) {
			log.warn("Processing of message {} from partition {} failed after {} attempts", messageAndMetadata.offset(),
				messageAndMetadata.partition(), retryCount, latestException);
			return;
		}
		if (latestException != null) {
			throw latestException;
		}
	}
}
