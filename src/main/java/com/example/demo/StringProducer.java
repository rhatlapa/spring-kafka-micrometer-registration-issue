package com.example.demo;

import org.springframework.kafka.core.KafkaTemplate;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class StringProducer {

	private final KafkaTemplate<String, String> kafkaTemplate;
	private final String topicName;

	public void publish(String key, String msg) {
		kafkaTemplate.send(topicName, key, msg)
				.whenComplete((result, error) -> {
					if (error != null) {
						logFailure(msg, error);
					} else {
						logSuccess(msg);
					}
				});
	}

	private void logSuccess(String msg) {
		log.trace("Successfully published '{}' into topic '{}'",
				msg, topicName);
	}

	private void logFailure(String msg, Throwable throwable) {
		log.warn("Exception occurred while publishing msg '{}' into topic '{}'.",
				msg, topicName, throwable);
	}
}
