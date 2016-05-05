package smartthings.konsumer.circuitbreaker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import smartthings.konsumer.event.KonsumerEvent;
import smartthings.konsumer.event.KonsumerEventListener;
import smartthings.konsumer.util.ThreadFactoryBuilder;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.util.*;
import java.util.concurrent.*;

@ThreadSafe
public class SimpleCircuitBreaker implements CircuitBreaker {

	private final static Logger log = LoggerFactory.getLogger(SimpleCircuitBreaker.class);

	private final Object mutex = new Object();

	@GuardedBy("mutex")
	private boolean open = false;

	@GuardedBy("mutex")
	private final Set<String> sources = new HashSet<>();

	private final List<KonsumerEventListener> listeners = new CopyOnWriteArrayList<>();

	private final ExecutorService callbackExecutor;

	public SimpleCircuitBreaker() {
		this(Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
			.setDaemon(true)
			.setNameFormat("KafkaCircuitBreaker-callback")
			.build())
		);
	}

	public SimpleCircuitBreaker(ExecutorService callbackExecutor) {
		this.callbackExecutor = callbackExecutor;
	}

	@Override
	public void blockIfOpen() {
		synchronized (mutex) {
			while (open) {
				try {
					log.debug("SimpleCircuitBreaker.blockIfOpen - calling wait - {}", Thread.currentThread().getName());
					mutex.wait();
				} catch (InterruptedException e) {
					//Intentionally left blank
				}
			}
		}
	}

	@Override
	public boolean isOpen() {
		synchronized (mutex) {
			return open;
		}
	}

	@Override
	public void open(String sourceId) {
		synchronized (mutex) {
			open = true;
			if (sources.isEmpty()) {
				log.info("CircuitBreaker opened by {}", sourceId);
				sources.add(sourceId);
				notifyListeners(KonsumerEvent.STOPPED);
			} else if (sources.contains(sourceId)) {
				log.info("CircuitBreaker has already been opened by {}", sourceId);
			} else {
				log.info("CircuitBreaker already open when open requested by {}", sourceId);
				sources.add(sourceId);
			}
		}
	}

	@Override
	public void conditionalClose(String sourceId) {
		synchronized (mutex) {
			if (sources.size() == 1 && sources.contains(sourceId)) {
				open = false;
				sources.remove(sourceId);
				notifyListeners(KonsumerEvent.RESUMED);
				log.info("conditionalClose - Closing - {}", Thread.currentThread().getName());
				mutex.notifyAll();
			} else {
				sources.remove(sourceId);
			}
		}
	}

	@Override
	public void addListener(KonsumerEventListener listener) {
		listeners.add(listener);
	}

	/**
	 * The listener will call the listener in a separate trade to avoid running the callbacks while holding the lock
	 * on the circuit breaker.
	 */
	private void notifyListeners(final KonsumerEvent event) {
		for(final KonsumerEventListener listener : listeners) {
			try {
				callbackExecutor.submit(new CircuitBreakerEventCallbackTask(listener, event));
			} catch (RejectedExecutionException e) {
				log.warn("Submission of task to executor was rejected", e);
			}
		}
	}

	private static class CircuitBreakerEventCallbackTask implements Runnable {

		private final KonsumerEventListener listener;
		private final KonsumerEvent event;

		public CircuitBreakerEventCallbackTask(KonsumerEventListener listener, KonsumerEvent event) {
			this.listener = listener;
			this.event = event;
		}

		@Override
		public void run() {
			try {
				listener.eventNotification(event);
			} catch (Exception e) {
				log.warn("Exception when notifying listeners of circuit breaker event", e);
			}
		}
	}
}
