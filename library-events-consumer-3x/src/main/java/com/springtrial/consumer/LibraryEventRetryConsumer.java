package com.springtrial.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class LibraryEventRetryConsumer {
  @Value("${kafka-retry-topic}")
  private String retryTopic;

  @KafkaListener(topics = "${kafka-retry-topic}", groupId = "retry-listener-group", autoStartup = "${retryListener-autostart:true}")
  public void retryListener(ConsumerRecord<Integer, String> consumerRecord) {
    log.info("LibraryEventRetryConsumer Consumer record  received from {} :{}", consumerRecord.topic(), consumerRecord.value());
    log.info("Header info from consumer record");
    consumerRecord.headers().forEach(header -> log.info("{} : {}", header.key(), new String(header.value())));
  }
}
