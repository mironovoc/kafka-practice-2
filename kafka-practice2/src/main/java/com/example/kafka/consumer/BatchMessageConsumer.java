package com.example.kafka.consumer;

import com.example.kafka.config.KafkaProperties;
import com.example.kafka.model.EventMessage;
import com.example.kafka.serde.EventMessageSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

@Component
public class BatchMessageConsumer implements Runnable, AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(BatchMessageConsumer.class);

	public static final String GROUP_ID = "batch-message-consumer-group";

	private final KafkaProperties kafkaProperties;
	private volatile boolean running = true;
	private KafkaConsumer<String, EventMessage> consumer;

	public BatchMessageConsumer(KafkaProperties kafkaProperties) {
		this.kafkaProperties = kafkaProperties;
	}

	public void start() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, EventMessageSerde.DeserializerImpl.class.getName());

		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
		// min bytes за fetch - брокер может подождать, пока накопится data
		props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1024);
		// если min bytes не набрали - ждём не дольше этого времени (мс)
		props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList(kafkaProperties.getTopic()));
		log.info("BatchMessageConsumer started, groupId={}", GROUP_ID);
	}

	@Override
	public void run() {
		if (consumer == null) {
			start();
		}
		while (running) {
			try {
				List<ConsumerRecord<String, EventMessage>> batch = pollAtLeast(10);
				if (batch.isEmpty()) {
					continue;
				}
				for (ConsumerRecord<String, EventMessage> record : batch) {
					EventMessage value = record.value();
					if (value == null) {
						System.err.println("[BatchMessageConsumer] Ошибка десериализации, offset="
								+ record.offset());
						continue;
					}
					System.out.println("[BatchMessageConsumer] Получено: " + value + " (partition="
							+ record.partition() + ", offset=" + record.offset() + ")");
					process(value);
				}
				consumer.commitSync(); // один коммит на всю пачку
				log.info("BatchMessageConsumer: закоммичена пачка из {} сообщений", batch.size());
			} catch (Exception e) {
				log.error("BatchMessageConsumer: ошибка poll/обработки/коммита: {}", e.getMessage(), e);
			}
		}
	}

	private List<ConsumerRecord<String, EventMessage>> pollAtLeast(int minRecords) {
		List<ConsumerRecord<String, EventMessage>> accumulated = new ArrayList<>();
		long deadline = System.currentTimeMillis() + 30_000;
		while (accumulated.size() < minRecords && System.currentTimeMillis() < deadline && running) {
			ConsumerRecords<String, EventMessage> records = consumer.poll(Duration.ofMillis(1000));
			records.forEach(accumulated::add);
		}
		return accumulated;
	}

	private void process(EventMessage message) {
		log.debug("Обработано batch: id={}", message.getId());
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
