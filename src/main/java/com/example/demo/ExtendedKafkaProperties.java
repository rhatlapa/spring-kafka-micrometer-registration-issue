package com.example.demo;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.CommonClientConfigs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import lombok.Data;

@ConfigurationProperties("extended.kafka")
@Data
public class ExtendedKafkaProperties {

	private final KafkaProperties kafkaProperties;
	private final SslBundles sslBundles;

	private int maxFailures;

	private Map<String, KafkaProperties.Admin> admins = new HashMap<>();
	private Map<String, KafkaProperties.Consumer> consumers = new HashMap<>();
	private Map<String, KafkaProperties.Producer> producers = new HashMap<>();
	private Map<String, KafkaProperties.Listener> listeners = new HashMap<>();
	private Map<String, KafkaProperties.Streams> streams = new HashMap<>();

	@Autowired
	public ExtendedKafkaProperties(KafkaProperties kafkaProperties, SslBundles sslBundles) {
		this.kafkaProperties = kafkaProperties;
		this.sslBundles = sslBundles;
	}

	public void setAdmins(Map<String, KafkaProperties.Admin> admins) {
		this.admins = new HashMap<>(admins);
	}

	/**
	 * Create an initial map of admin properties from the state of this instance.
	 * <p>
	 * This allows you to add additional properties, if necessary, and override the
	 * default KafkaAdmin bean.
	 *
	 * @return the admin properties initialized with the customizations defined on this
	 * instance
	 */
	public Map<String, Object> buildAdminProperties(String configKey, String bootstrapServers) {
		Map<String, Object> properties = kafkaProperties.buildAdminProperties(sslBundles);
		if (bootstrapServers != null) {
			properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		}
		Optional.ofNullable(admins.get(configKey))
				.map(admin -> admin.buildProperties(sslBundles))
				.ifPresent(properties::putAll);
		return properties;
	}

	/**
	 * Create an initial map of consumer properties from the state of this instance.
	 * <p>
	 * This allows you to add additional properties, if necessary, and override the
	 * default kafkaConsumerFactory bean.
	 *
	 * @return the consumer properties initialized with the customizations defined on this
	 * instance
	 */
	public Map<String, Object> buildConsumerProperties(String configKey) {
		Map<String, Object> properties = kafkaProperties.buildConsumerProperties(sslBundles);
		Optional.ofNullable(consumers.get(configKey))
				.map(consumer -> consumer.buildProperties(sslBundles))
				.ifPresent(properties::putAll);
		return properties;
	}

	/**
	 * Create an initial map of producer properties from the state of this instance.
	 * <p>
	 * This allows you to add additional properties, if necessary, and override the
	 * default kafkaProducerFactory bean.
	 *
	 * @return the producer properties initialized with the customizations defined on this
	 * instance
	 */
	public Map<String, Object> buildProducerProperties(String configKey) {
		Map<String, Object> properties = kafkaProperties.buildProducerProperties(sslBundles);
		Optional.ofNullable(producers.get(configKey))
				.map(producer -> producer.buildProperties(sslBundles))
				.ifPresent(properties::putAll);
		return properties;
	}

	public Map<String, Object> buildStreamsProperties(String configKey) {
		Map<String, Object> properties = kafkaProperties.buildStreamsProperties(sslBundles);
		Optional.ofNullable(streams.get(configKey))
				.map(streams -> streams.buildProperties(sslBundles))
				.ifPresent(properties::putAll);
		return properties;
	}

	public KafkaStreamsConfiguration buildStreamsConfiguration(String configKey) {
		return new KafkaStreamsConfiguration(buildStreamsProperties(configKey));
	}

	public KafkaProperties.Listener getListener() {
		return kafkaProperties.getListener();
	}

	public KafkaProperties.Listener getListener(String configKey) {
		return Optional.ofNullable(listeners.get(configKey))
				.orElseGet(KafkaProperties.Listener::new);
	}
}

