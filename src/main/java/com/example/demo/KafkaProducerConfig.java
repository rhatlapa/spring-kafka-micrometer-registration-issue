package com.example.demo;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Configuration
public class KafkaProducerConfig {

	@NoArgsConstructor(access = AccessLevel.PRIVATE)
	public static class FirstProducerConfig {
		public static final String TOPIC_NAME = "first";
		public static final String CONFIG_KEY = "first";
		public static final String CLIENT_NAME = TOPIC_NAME + "-client";
		public static final String PRODUCER_FACTORY = TOPIC_NAME + "ProducerFactory";
		public static final String KAFKA_TEMPLATE = TOPIC_NAME + "KafkaTemplate";
		public static final String PRODUCER = TOPIC_NAME + "Producer";
	}

	@NoArgsConstructor(access = AccessLevel.PRIVATE)
	public static class SecondProducerConfig {

		public static final String TOPIC_NAME = "second";
		public static final String CONFIG_KEY = "second";
		public static final String CLIENT_NAME = TOPIC_NAME + "-client";
		public static final String PRODUCER_FACTORY = TOPIC_NAME + "ProducerFactory";
		public static final String KAFKA_TEMPLATE = TOPIC_NAME + "KafkaTemplate";
		public static final String PRODUCER = TOPIC_NAME + "Producer";
	}

	private final KafkaTemplateFactory kafkaTemplateFactory;
	private final ProducerAbstractFactory producerAbstractFactory;

	@Bean(name = SecondProducerConfig.KAFKA_TEMPLATE)
	public KafkaTemplate<String, String> secondKafkaTemplate(
			@Qualifier(SecondProducerConfig.PRODUCER_FACTORY) ProducerFactory<String, String> producerFactory) {
		return kafkaTemplateFactory.createKafkaTemplate(producerFactory);
	}

	@Bean(name = FirstProducerConfig.PRODUCER_FACTORY)
	public ProducerFactory<String, String> firstProducerFactory() {
		return producerAbstractFactory.newFactory(FirstProducerConfig.CONFIG_KEY, FirstProducerConfig.CLIENT_NAME);
	}

	@Bean(name = SecondProducerConfig.PRODUCER)
	public StringProducer secondProducer(
			@Qualifier(SecondProducerConfig.KAFKA_TEMPLATE) KafkaTemplate<String, String> secondKafkaTemplate) {
		return new StringProducer(secondKafkaTemplate, SecondProducerConfig.TOPIC_NAME);
	}

	@Bean(name = FirstProducerConfig.KAFKA_TEMPLATE)
	public KafkaTemplate<String, String> firstKafkaTemplate(
			@Qualifier(FirstProducerConfig.PRODUCER_FACTORY) ProducerFactory<String, String> producerFactory) {
		return kafkaTemplateFactory.createKafkaTemplate(producerFactory);
	}

	@Bean(name = FirstProducerConfig.PRODUCER)
	public StringProducer firstProducer(
			@Qualifier(FirstProducerConfig.KAFKA_TEMPLATE) KafkaTemplate<String, String> firstKafkaTemplate) {
		return new StringProducer(firstKafkaTemplate, FirstProducerConfig.TOPIC_NAME);
	}

	@Bean(name = SecondProducerConfig.PRODUCER_FACTORY)
	public ProducerFactory<String, String> secondProducerFactory() {
		return producerAbstractFactory.newFactory(SecondProducerConfig.CONFIG_KEY, SecondProducerConfig.CLIENT_NAME);
	}
}
