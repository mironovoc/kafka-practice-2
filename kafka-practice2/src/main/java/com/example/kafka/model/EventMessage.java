package com.example.kafka.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;

public final class EventMessage {

	private final long id;
	private final String payload;
	private final Instant createdAt;
	private final String sourceInstance;

	@JsonCreator
	public EventMessage(
			@JsonProperty("id") long id,
			@JsonProperty("payload") String payload,
			@JsonProperty("createdAt") Instant createdAt,
			@JsonProperty("sourceInstance") String sourceInstance) {
		this.id = id;
		this.payload = payload;
		this.createdAt = createdAt;
		this.sourceInstance = sourceInstance;
	}

	public EventMessage(long id, String payload, String sourceInstance) {
		this(id, payload, Instant.now(), sourceInstance);
	}

	public long getId() {
		return id;
	}

	public String getPayload() {
		return payload;
	}

	public Instant getCreatedAt() {
		return createdAt;
	}

	public String getSourceInstance() {
		return sourceInstance;
	}

	@Override
	public String toString() {
		return "EventMessage{id=" + id + ", payload='" + payload + "', createdAt=" + createdAt
				+ ", sourceInstance='" + sourceInstance + "'}";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof EventMessage that)) {
			return false;
		}
		return id == that.id && Objects.equals(payload, that.payload)
				&& Objects.equals(createdAt, that.createdAt)
				&& Objects.equals(sourceInstance, that.sourceInstance);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, payload, createdAt, sourceInstance);
	}
}
