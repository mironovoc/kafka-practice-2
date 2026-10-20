package com.example.kafka.consumer;

import com.example.kafka.config.KafkaProperties;
import com.example.kafka.model.EventMessage;
import com.example.kafka.serde.EventMessageSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Component
public class SingleMessageConsumer implements Runnable, AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(SingleMessageConsumer.class);

	public static final String GROUP_ID = "single-message-consumer-group";

	private final KafkaProperties kafkaProperties;
	private volatile boolean running = true;
	private KafkaConsumer<String, EventMessage> consumer;

	public SingleMessageConsumer(KafkaProperties kafkaProperties) {
		this.kafkaProperties = kafkaProperties;
	}

	public void start() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EventMessageSerde.DeserializerImpl.class.getName());

		// автокоммит offset после poll
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		// одно сообщение за poll
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList(kafkaProperties.getTopic()));
		log.info("SingleMessageConsumer started, groupId={}", GROUP_ID);
	}

	@Override
	public void run() {
		if (consumer == null) {
			start();
		}
		while (running) {
			try {
				ConsumerRecords<String, EventMessage> records = consumer.poll(Duration.ofMillis(1000));
				records.forEach(record -> {
					EventMessage value = record.value();
					if (value == null) {
						System.err.println("[SingleMessageConsumer] Пустое сообщение после десериализации, offset="
								+ record.offset());
						return;
					}
					System.out.println("[SingleMessageConsumer] Получено: " + value + " (partition="
							+ record.partition() + ", offset=" + record.offset() + ")");
					process(value);
				});
			} catch (Exception e) {
				log.error("SingleMessageConsumer: ошибка poll/обработки: {}", e.getMessage(), e);
			}
		}
	}

	private void process(EventMessage message) {
		log.debug("Обработано single: id={}", message.getId());
	}

	public void shutdown() {
		running = false;
		close();
	}

	@Override
	public void close() {
		if (consumer != null) {
			consumer.close();
			consumer = null;
		}
	}
}
