package smartthings.konsumer.circuitbreaker;

import smartthings.konsumer.event.KonsumerEventListener;

public interface CircuitBreaker {

	void blockIfOpen();

	boolean isOpen();

	void open(String sourceId);

	void conditionalClose(String sourceId);

	void addListener(KonsumerEventListener listener);

}
