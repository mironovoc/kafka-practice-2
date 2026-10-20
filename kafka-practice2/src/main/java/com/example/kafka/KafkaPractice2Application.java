package com.example.kafka;

import com.example.kafka.config.KafkaProperties;
import com.example.kafka.consumer.BatchMessageConsumer;
import com.example.kafka.consumer.SingleMessageConsumer;
import com.example.kafka.producer.MessageProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;

@SpringBootApplication
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaPractice2Application {

	private static final Logger log = LoggerFactory.getLogger(KafkaPractice2Application.class);

	public static void main(String[] args) {
		ConfigurableApplicationContext context = SpringApplication.run(KafkaPractice2Application.class, args);

		MessageProducer producer = context.getBean(MessageProducer.class);
		SingleMessageConsumer singleConsumer = context.getBean(SingleMessageConsumer.class);
		BatchMessageConsumer batchConsumer = context.getBean(BatchMessageConsumer.class);

		producer.start();
		singleConsumer.start();
		batchConsumer.start();

		Thread producerThread = new Thread(producer, "message-producer");
		Thread singleThread = new Thread(singleConsumer, "single-message-consumer");
		Thread batchThread = new Thread(batchConsumer, "batch-message-consumer");

		producerThread.setDaemon(false);
		singleThread.setDaemon(false);
		batchThread.setDaemon(false);

		producerThread.start();
		singleThread.start();
		batchThread.start();

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			log.info("Shutdown: останавливаем продюсер и консьюмеры...");
			producer.shutdown();
			singleConsumer.shutdown();
			batchConsumer.shutdown();
		}));

		log.info("Приложение запущено: 1 producer + SingleMessageConsumer + BatchMessageConsumer");
	}
}
