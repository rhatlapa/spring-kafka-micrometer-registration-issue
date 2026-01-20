package com.example.demo;

import java.time.Duration;
import java.util.function.Consumer;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties.Listener;
import org.springframework.boot.context.properties.PropertyMapper;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;

public class ConcurrentKafkaListenerContainerAbstractFactory {

	private final ExtendedKafkaProperties extendedKafkaProperties;
	private final CommonErrorHandler errorHandler;


	public ConcurrentKafkaListenerContainerAbstractFactory(ExtendedKafkaProperties extendedKafkaProperties) {
		this.extendedKafkaProperties = extendedKafkaProperties;
		this.errorHandler = new DefaultErrorHandler();
	}

	public <K, V> ConcurrentKafkaListenerContainerFactory<K, V> createListenerContainerFactory(String configKey,
			ConsumerFactory<K, V> customConsumerFactory) {
		return createListenerContainerFactory(configKey, customConsumerFactory, containerProperties -> {
		});
	}

	public <K, V> ConcurrentKafkaListenerContainerFactory<K, V> createListenerContainerFactory(String configKey,
			ConsumerFactory<K, V> customConsumerFactory, Consumer<ContainerProperties> configurer) {

		ConcurrentKafkaListenerContainerFactory<K, V> listenerContainerFactory =
				new ConcurrentKafkaListenerContainerFactory<>();

		configureListenerFactoryWithOverrides(configKey, customConsumerFactory, listenerContainerFactory);
		configureContainerWithOverrides(configKey, configurer, listenerContainerFactory);

		return listenerContainerFactory;
	}

	private <K, V> void configureListenerFactoryWithOverrides(String configKey,
			ConsumerFactory<K, V> customConsumerFactory,
			ConcurrentKafkaListenerContainerFactory<K, V> listenerContainerFactory) {
		configureListenerFactory(listenerContainerFactory, customConsumerFactory,
				extendedKafkaProperties.getListener());
		configureListenerFactory(listenerContainerFactory, customConsumerFactory,
				extendedKafkaProperties.getListener(configKey));
	}

	private <K, V> void configureContainerWithOverrides(String configKey, Consumer<ContainerProperties> configurer,
			ConcurrentKafkaListenerContainerFactory<K, V> listenerContainerFactory) {
		configureContainer(listenerContainerFactory.getContainerProperties(),
				extendedKafkaProperties.getListener(), configurer);
		configureContainer(listenerContainerFactory.getContainerProperties(),
				extendedKafkaProperties.getListener(configKey), configurer);
	}

	private <K, V> void configureListenerFactory(ConcurrentKafkaListenerContainerFactory<K, V> listenerContainerFactory,
			ConsumerFactory<K, V> consumerFactory, Listener properties) {
		PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();
		map.from(properties::getConcurrency).to(listenerContainerFactory::setConcurrency);
		map.from(consumerFactory).to(listenerContainerFactory::setConsumerFactory);
		map.from(this.errorHandler).to(listenerContainerFactory::setCommonErrorHandler);
	}

	private static void configureContainer(ContainerProperties container, Listener properties,
			Consumer<ContainerProperties> configurer) {
		PropertyMapper map = PropertyMapper.get().alwaysApplyingWhenNonNull();
		map.from(properties::getAckMode).to(container::setAckMode);
		map.from(properties::getClientId).to(container::setClientId);
		map.from(properties::getAckCount).to(container::setAckCount);
		map.from(properties::getAckTime).as(Duration::toMillis).to(container::setAckTime);
		map.from(properties::getPollTimeout).as(Duration::toMillis).to(container::setPollTimeout);
		map.from(properties::getNoPollThreshold).to(container::setNoPollThreshold);
		map.from(properties::getIdleEventInterval).as(Duration::toMillis).to(container::setIdleEventInterval);
		map.from(properties::getMonitorInterval).as(Duration::getSeconds).as(Number::intValue)
				.to(container::setMonitorInterval);
		map.from(properties::getLogContainerConfig).to(container::setLogContainerConfig);
		container.setObservationEnabled(true);
		configurer.accept(container);
	}

}
