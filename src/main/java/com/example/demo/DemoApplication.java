package com.example.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@Configuration
@EnableKafka
@EnableConfigurationProperties(ExtendedKafkaProperties.class)
public class DemoApplication {

    static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }

    @Slf4j
    @KafkaListener(id = KafkaListenersConfig.FirstListenerConfig.ID,  topics = { KafkaListenersConfig.FirstListenerConfig.TOPIC_NAME}, groupId = "${spring.kafka.consumer.group-id}",
            idIsGroup = false, containerFactory = KafkaListenersConfig.FirstListenerConfig.CONTAINER)
    public static class FirstKafkaEventListener {

        @KafkaHandler
        public void onMessage(
                @Header(KafkaHeaders.RECEIVED_KEY) String key,
                @Payload String msg) {
            log.info("received regular event: {} - {}", key, msg);
        }
    }

    @Slf4j
    @KafkaListener(id = KafkaListenersConfig.SecondListenerConfig.ID,  topics = { KafkaListenersConfig.SecondListenerConfig.TOPIC_NAME}, groupId = "${spring.kafka.consumer.group-id}",
            idIsGroup = false, containerFactory = KafkaListenersConfig.SecondListenerConfig.CONTAINER)
    public static class SecondKafkaEventListener {

        @KafkaHandler
        public void onMessage(
                @Header(KafkaHeaders.RECEIVED_KEY) String key,
                @Payload String msg) {
            log.info("received regular event: {} - {}", key, msg);
        }
    }
}
