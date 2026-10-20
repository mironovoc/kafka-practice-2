package com.example.kafka.serde;

import com.example.kafka.model.EventMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public final class EventMessageSerde {

	private static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

	private EventMessageSerde() {
	}

	public static class SerializerImpl implements Serializer<EventMessage> {

		private static final Logger log = LoggerFactory.getLogger(SerializerImpl.class);

		@Override
		public byte[] serialize(String topic, EventMessage data) {
			if (data == null) {
				return null;
			}
			try {
				return MAPPER.writeValueAsBytes(data);
			} catch (Exception e) {
				log.error("Ошибка сериализации EventMessage для топика {}: {}", topic, e.getMessage(), e);
				System.err.println("Ошибка сериализации: " + e.getMessage());
				return null;
			}
		}

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
            Serializer.super.configure(configs, isKey);
        }

		@Override
		public void close() {
            Serializer.super.close();
        }
	}

	public static class DeserializerImpl implements Deserializer<EventMessage> {

		private static final Logger log = LoggerFactory.getLogger(DeserializerImpl.class);

		@Override
		public EventMessage deserialize(String topic, byte[] data) {
			if (data == null) {
				return null;
			}
			try {
				return MAPPER.readValue(data, EventMessage.class);
			} catch (Exception e) {
				log.error("Ошибка десериализации EventMessage из топика {}: {}", topic, e.getMessage(), e);
				System.err.println("Ошибка десериализации: " + e.getMessage());
				return null;
			}
		}

		@Override
		public void configure(Map<String, ?> configs, boolean isKey) {
            Deserializer.super.configure(configs, isKey);
        }

		@Override
		public void close() {
            Deserializer.super.close();
        }
	}

	public static String toJsonForConsole(EventMessage message) {
		try {
			return new String(MAPPER.writeValueAsBytes(message), StandardCharsets.UTF_8);
		} catch (Exception e) {
			return String.valueOf(message);
		}
	}
}
