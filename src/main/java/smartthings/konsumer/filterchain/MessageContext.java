package smartthings.konsumer.filterchain;

import kafka.message.MessageAndMetadata;
import smartthings.konsumer.circuitbreaker.CircuitBreaker;

public interface MessageContext {

	void next(MessageAndMetadata<byte[], byte[]> messageAndMetadata) throws Exception;

	CircuitBreaker getCircuitBreaker();

}
