package com.example.demo;

import java.util.List;

import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.MicrometerProducerListener;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;

@Component
public class ProducerAbstractFactory {

	private final String CLIENT_TAG = "client";

	private final ExtendedKafkaProperties extendedKafkaProperties;
	private final MeterRegistry meterRegistry;

	public ProducerAbstractFactory(ExtendedKafkaProperties extendedKafkaProperties, MeterRegistry meterRegistry) {
		this.extendedKafkaProperties = extendedKafkaProperties;
		this.meterRegistry = meterRegistry;
	}

	public ProducerFactory<String, String>newFactory(String configKey, String clientId) {
		var producerFactory = new DefaultKafkaProducerFactory<>(extendedKafkaProperties.buildProducerProperties(configKey),
				new StringSerializer(), new StringSerializer());
		producerFactory.addListener(new MicrometerProducerListener<>(meterRegistry, List.of(Tag.of(CLIENT_TAG, clientId))));
		return producerFactory;
	}
}

