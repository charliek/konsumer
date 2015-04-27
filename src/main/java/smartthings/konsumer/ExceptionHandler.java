package smartthings.konsumer;

public interface ExceptionHandler {
	void handleException(MessageEnvelope msg, Throwable t);
}
