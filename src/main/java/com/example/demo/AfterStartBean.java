package com.example.demo;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class AfterStartBean {

	private final StringProducer firstProducer;
	private final StringProducer secondProducer;

	@Autowired
	public AfterStartBean(@Qualifier(KafkaProducerConfig.FirstProducerConfig.PRODUCER) StringProducer firstProducer,
			@Qualifier(KafkaProducerConfig.SecondProducerConfig.PRODUCER) StringProducer secondProducer) {
		this.firstProducer = firstProducer;
		this.secondProducer = secondProducer;
	}

	@EventListener(ApplicationReadyEvent.class)
	public void onApplicationEvent(ApplicationReadyEvent event) {
		log.info("Application context started, sending startup messages to Kafka topics.");
		firstProducer.publish("first", "started");
		secondProducer.publish("second", "started");
	}
}
