package com.example.demo;

import java.util.List;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@Configuration
@AllArgsConstructor
public class KafkaListenersConfig {

	private ExtendedKafkaProperties extendedKafkaProperties;
	private MeterRegistry meterRegistry;

	public static class BeanQualifiers {

		public static final String DEFAULT_COMMON_ERROR_HANDLER = "defaultCommonErrorHandler";

		private BeanQualifiers() {
			throw new IllegalStateException("Utility class");
		}
	}


	@NoArgsConstructor(access = AccessLevel.PRIVATE)
	public static class FirstListenerConfig {

		public static final String FIRST = "first";

		public static final String TOPIC_NAME = FIRST;
		public static final String ID = FIRST + "-listener";
		public static final String CONFIG_KEY = FIRST;
		public static final String CONTAINER = FIRST;
		public static final String CLIENT_ID = "first-client";
	}

	@NoArgsConstructor(access = AccessLevel.PRIVATE)
	public static class SecondListenerConfig {

		public static final String SECOND = "second";

		public static final String TOPIC_NAME = SECOND;
		public static final String ID = SECOND + "-listener";
		public static final String CONFIG_KEY = SECOND;
		public static final String CONTAINER = SECOND;
		public static final String CLIENT_ID = "second-client";
	}

	@Bean
	public ConcurrentKafkaListenerContainerAbstractFactory concurrentKafkaListenerContainerAbstractFactory(ExtendedKafkaProperties extendedKafkaProperties) {
		return new ConcurrentKafkaListenerContainerAbstractFactory(extendedKafkaProperties);
	}

	@Bean(FirstListenerConfig.CONTAINER)
	public ConcurrentKafkaListenerContainerFactory<String, String> firstContainerFactory(
			ConcurrentKafkaListenerContainerAbstractFactory concurrentKafkaListenerContainerAbstractFactory) {
		var consumerFactory = createStringConsumerFactory(FirstListenerConfig.CONFIG_KEY, FirstListenerConfig.CLIENT_ID);
		return concurrentKafkaListenerContainerAbstractFactory.createListenerContainerFactory(
				FirstListenerConfig.CONFIG_KEY,
				consumerFactory);
	}

	@Bean(SecondListenerConfig.CONTAINER)
	public ConcurrentKafkaListenerContainerFactory<String, String> secondContainerFactory(
			ConcurrentKafkaListenerContainerAbstractFactory concurrentKafkaListenerContainerAbstractFactory) {
		var consumerFactory = createStringConsumerFactory(SecondListenerConfig.CONFIG_KEY, SecondListenerConfig.CLIENT_ID);
		return concurrentKafkaListenerContainerAbstractFactory.createListenerContainerFactory(
				SecondListenerConfig.CONFIG_KEY,
				consumerFactory);
	}

	@Bean
	public DemoApplication.FirstKafkaEventListener firstKafkaEventListener() {
		return new DemoApplication.FirstKafkaEventListener();
	}

	@Bean
	public DemoApplication.SecondKafkaEventListener secondKafkaEventListener() {
		return new DemoApplication.SecondKafkaEventListener();
	}

	@Bean(name = BeanQualifiers.DEFAULT_COMMON_ERROR_HANDLER)
	public CommonErrorHandler commonErrorHandler() {
		return new DefaultErrorHandler(new FixedBackOff(0, extendedKafkaProperties.getMaxFailures() - 1));
	}

	private ConsumerFactory<String, String> createStringConsumerFactory(String configKey, String clientId) {
		var consumerFactory = new DefaultKafkaConsumerFactory<>(extendedKafkaProperties.buildConsumerProperties(configKey),
				new StringDeserializer(), new ErrorHandlingDeserializer<>(new StringDeserializer()));
		consumerFactory.addListener(new MicrometerConsumerListener<>(meterRegistry, List.of(Tag.of("client", clientId))));
		return consumerFactory;
	}
}
