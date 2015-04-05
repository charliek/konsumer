package com.charlieknudsen.konsumer.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class RunUtil {

	private final static Logger log = LoggerFactory.getLogger(RunUtil.class);

	static public void blockForShutdown(final QuietCallable onShutdown) {
		final CountDownLatch shutdownLatch = new CountDownLatch(1);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				log.warn("Beginning to shutdown kafka");
				onShutdown.call();
				log.warn("Kafka shutdown complete.");
				shutdownLatch.countDown();
			}
		});
		try {
			shutdownLatch.await();
		} catch (InterruptedException e) {
			log.warn("Interrupted when reading kafka", e);
		}
	}

}
