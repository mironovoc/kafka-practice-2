package com.example.kafka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "kafka")
public class KafkaProperties {

	private String bootstrapServers = "localhost:9092";
	private String topic = "my-topic";
	private Producer producer = new Producer();

	public String getBootstrapServers() {
		return bootstrapServers;
	}

	public void setBootstrapServers(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Producer getProducer() {
		return producer;
	}

	public void setProducer(Producer producer) {
		this.producer = producer;
	}

	public static class Producer {
		private long intervalMs = 500;

		public long getIntervalMs() {
			return intervalMs;
		}

		public void setIntervalMs(long intervalMs) {
			this.intervalMs = intervalMs;
		}
	}
}
