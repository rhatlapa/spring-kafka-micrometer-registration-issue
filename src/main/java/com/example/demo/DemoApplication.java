package com.example.demo;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@Configuration
@EnableKafka
@EnableConfigurationProperties(ExtendedKafkaProperties.class)
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }
    
    
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class FirstListenerConfig {

        public static final String FIRST = "first";

        public static final String TOPIC_NAME = FIRST;
        public static final String ID = FIRST + "-listener";
        public static final String CONFIG_KEY = FIRST;
        public static final String CONTAINER = FIRST;
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class SecondListenerConfig {

        public static final String SECOND = "second";

        public static final String TOPIC_NAME = SECOND;
        public static final String ID = SECOND + "-listener";
        public static final String CONFIG_KEY = SECOND;
        public static final String CONTAINER = SECOND;
    }


    @Bean
    public FirstKafkaEventListener firstKafkaEventListener() {
        return new FirstKafkaEventListener();
    }

    @Bean
    public SecondKafkaEventListener secondKafkaEventListener() {
        return new SecondKafkaEventListener();
    }

    @Slf4j
    public static class FirstKafkaEventListener {

        @KafkaListener(id = FirstListenerConfig.ID,  topics = {FirstListenerConfig.TOPIC_NAME}, groupId = "${spring.kafka.consumer.group-id}",
                idIsGroup = false, containerFactory = FirstListenerConfig.CONTAINER)
        public void handleEvent(String msg) {
            log.info("received regular event: {}", msg);
        }
    }

    @Slf4j
    public static class SecondKafkaEventListener {

        @KafkaListener(id = SecondListenerConfig.ID,  topics = {SecondListenerConfig.TOPIC_NAME}, groupId = "${spring.kafka.consumer.group-id}",
                idIsGroup = false, containerFactory = SecondListenerConfig.CONTAINER)
        public void handleEvent(String msg) {
            log.info("received regular event: {}", msg);
        }
    }

    @Bean
    public ConcurrentKafkaListenerContainerAbstractFactory concurrentKafkaListenerContainerAbstractFactory(ExtendedKafkaProperties extendedKafkaProperties) {
        return new ConcurrentKafkaListenerContainerAbstractFactory(extendedKafkaProperties);
    }

    @Bean(FirstListenerConfig.CONTAINER)
    public ConcurrentKafkaListenerContainerFactory<String, String> firstContainerFactory(
            ConcurrentKafkaListenerContainerAbstractFactory concurrentKafkaListenerContainerAbstractFactory,
            ExtendedKafkaProperties extendedKafkaProperties) {
        var consumerFactory = createStringConsumerFactory(FirstListenerConfig.CONFIG_KEY, extendedKafkaProperties);
        return concurrentKafkaListenerContainerAbstractFactory.createListenerContainerFactory(
                FirstListenerConfig.CONFIG_KEY,
                consumerFactory);
    }

    @Bean(SecondListenerConfig.CONTAINER)
    public ConcurrentKafkaListenerContainerFactory<String, String> secondContainerFactory(
            ConcurrentKafkaListenerContainerAbstractFactory concurrentKafkaListenerContainerAbstractFactory,
            ExtendedKafkaProperties extendedKafkaProperties) {
        var consumerFactory = createStringConsumerFactory(SecondListenerConfig.CONFIG_KEY, extendedKafkaProperties);
        return concurrentKafkaListenerContainerAbstractFactory.createListenerContainerFactory(
                SecondListenerConfig.CONFIG_KEY,
                consumerFactory);
    }


    private ConsumerFactory<String, String> createStringConsumerFactory(String configKey, ExtendedKafkaProperties extendedKafkaProperties) {
        return new DefaultKafkaConsumerFactory<> (extendedKafkaProperties.buildConsumerProperties(configKey),
                new StringDeserializer(), new ErrorHandlingDeserializer<>(new StringDeserializer()));
    }
}
