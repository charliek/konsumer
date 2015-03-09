package com.charlieknudsen.konsumer;

public interface ExceptionHandler {
	void handleException(MessageEnvelope msg, Throwable t);
}
