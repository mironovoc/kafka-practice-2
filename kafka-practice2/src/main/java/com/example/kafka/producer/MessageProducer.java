package com.example.kafka.producer;

import com.example.kafka.config.KafkaProperties;
import com.example.kafka.model.EventMessage;
import com.example.kafka.serde.EventMessageSerde;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class MessageProducer implements Runnable, AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(MessageProducer.class);

	private final KafkaProperties kafkaProperties;
	private final String instanceId;
	private final AtomicLong sequence = new AtomicLong();
	private volatile boolean running = true;
	private KafkaProducer<String, EventMessage> producer;

	public MessageProducer(KafkaProperties kafkaProperties) {
		this.kafkaProperties = kafkaProperties;
		this.instanceId = System.getenv().getOrDefault("HOSTNAME", "local-" + ProcessHandle.current().pid());
	}

	public void start() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, EventMessageSerde.SerializerImpl.class.getName());

		// acks=all - ждём все in-sync реплики (at least once по заданию)
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		// retries - повтор send при временных ошибках
		props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
		props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

		producer = new KafkaProducer<>(props);
		log.info("Producer started, bootstrap={}, topic={}", kafkaProperties.getBootstrapServers(),
				kafkaProperties.getTopic());
	}

	@Override
	public void run() {
		if (producer == null) {
			start();
		}
		while (running) {
			try {
				long id = sequence.incrementAndGet();
				EventMessage message = new EventMessage(id, "event-" + id, instanceId);
				String json = EventMessageSerde.toJsonForConsole(message);
				System.out.println("[Producer] Отправка: " + json);

				ProducerRecord<String, EventMessage> record = new ProducerRecord<>(kafkaProperties.getTopic(),
						String.valueOf(id), message);

				producer.send(record, (metadata, exception) -> {
					if (exception != null) {
						log.error("Ошибка доставки сообщения id={}: {}", id, exception.getMessage(), exception);
					} else {
						log.debug("Доставлено id={} partition={} offset={}", id, metadata.partition(),
								metadata.offset());
					}
				});

				Thread.sleep(kafkaProperties.getProducer().getIntervalMs());
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			} catch (Exception e) {
				log.error("Ошибка в цикле продюсера: {}", e.getMessage(), e);
			}
		}
	}

	public void shutdown() {
		running = false;
		close();
	}

	@Override
	public void close() {
		if (producer != null) {
			producer.flush();
			producer.close();
			producer = null;
		}
	}
}
