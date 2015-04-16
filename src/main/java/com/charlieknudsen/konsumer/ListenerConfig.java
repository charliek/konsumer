package com.charlieknudsen.konsumer;

import kafka.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

public class ListenerConfig {
	private final static Logger log = LoggerFactory.getLogger(ListenerConfig.class);

	private final int partitionThreads;
	private final int processingThreads;
	private final int processingQueueSize;
	private final int tryCount;
	private final int shutdownAwaitSeconds;
	private final boolean daemonThreads;
	private final String topic;
	private final Properties props;

	public ListenerConfig(Builder builder) {
		partitionThreads = builder.partitionThreads;
		processingThreads = builder.processingThreads;
		processingQueueSize = builder.processingQueueSize;
		tryCount = builder.tryCount;
		shutdownAwaitSeconds = builder.shutdownAwaitSeconds;
		topic = builder.topic;
		props = builder.props;
		daemonThreads = builder.daemonThreads;
	}

	public int getProcessingThreads() {
		return processingThreads;
	}

	public int getPartitionThreads() {
		return partitionThreads;
	}

	public int getProcessingQueueSize() {
		return processingQueueSize;
	}

	public int getShutdownAwaitSeconds() {
		return shutdownAwaitSeconds;
	}

	public int getTryCount() {
		return tryCount;
	}

	public String getTopic() {
		return topic;
	}

	public boolean useDaemonThreads() {
		return daemonThreads;
	}

	public ConsumerConfig getConsumerConfig() {
		return new ConsumerConfig(props);
	}

	public void dumpConfig() {
		log.info("Configuration for topic '{}", topic);
		log.info("topic {} - property 'partitionThreads' : {}", topic, partitionThreads);
		log.info("topic {} - property 'processingThreads' : {}", topic, processingThreads);
		log.info("topic {} - property 'processingQueueSize' : {}", topic, processingQueueSize);
		log.info("topic {} - property 'tryCount' : {}", topic, tryCount);
		log.info("topic {} - property 'shutdownAwaitSeconds' : {}", topic, shutdownAwaitSeconds);
		for (Map.Entry e : props.entrySet()) {
			log.info("topic {} - property '{}' : {}", topic, e.getKey(), e.getValue());
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	public static class Builder {
		private int partitionThreads = 1;
		private int processingThreads = 10;
		private int processingQueueSize = 20;
		private int tryCount = 2;
		private int shutdownAwaitSeconds = 4;
		private String topic = "";
		private Properties props = new Properties();
		private boolean daemonThreads = false;

		public Builder() {
			// http://kafka.apache.org/08/configuration.html
			// set default properties. Note these are the documented defaults.
			props.put("auto.commit.enable", "true");
			props.put("auto.commit.interval.ms", "60000");
			props.put("auto.offset.reset", "largest"); // smallest to start at beginning
			props.put("queued.max.message.chunks", "10");
			props.put("rebalance.max.retries", "4");
			props.put("zookeeper.session.timeout.ms", "6000");
			props.put("zookeeper.sync.time.ms", "2000");
		}

		public Builder partitionThreads(int count) {
			partitionThreads = count;
			return this;
		}

		public Builder processingThreads(int count) {
			processingThreads = count;
			return this;
		}

		public Builder processingQueueSize(int size) {
			processingQueueSize = size;
			return this;
		}

		public Builder tryCount(int count) {
			tryCount = count;
			return this;
		}

		public Builder shutdownAwaitSeconds(int seconds) {
			shutdownAwaitSeconds = seconds;
			return this;
		}

		public Builder consumerGroup(String name) {
			props.put("group.id", name);
			return this;
		}

		public Builder daemonThreads(boolean daemon) {
			this.daemonThreads = daemon;
			return this;
		}

		public Builder topic(String name) {
			topic = name;
			return this;
		}

		public Builder zookeeper(String connection) {
			props.put("zookeeper.connect", connection);
			return this;
		}

		public Builder setProperty(String key, String value) {
			props.put(key, value);
			return this;
		}

		public ListenerConfig build() {
			return new ListenerConfig(this);
		}
	}
}
