package com.example.demo;

import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

@Component
public class KafkaTemplateFactory {

	private final KafkaAdmin kafkaAdmin;

	public KafkaTemplateFactory(KafkaAdmin kafkaAdmin) {
		this.kafkaAdmin = kafkaAdmin;
	}

	public <K, V> KafkaTemplate<K, V> createKafkaTemplate(ProducerFactory<K, V> producerFactory) {
		var kafkaTemplate = new KafkaTemplate<>(producerFactory);
		kafkaTemplate.setObservationEnabled(true);
		kafkaTemplate.setKafkaAdmin(kafkaAdmin);
		return kafkaTemplate;
	}
}