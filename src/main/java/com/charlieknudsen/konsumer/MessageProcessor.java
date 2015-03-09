package com.charlieknudsen.konsumer;

public interface MessageProcessor {
	void processMessage(byte[] bytes) throws Exception;
}
