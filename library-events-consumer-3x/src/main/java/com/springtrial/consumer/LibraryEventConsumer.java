package com.springtrial.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.springtrial.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@ConditionalOnProperty(name = "manual-commit", havingValue = "false")
@Component
@Slf4j
public class LibraryEventConsumer {

  @Autowired
  LibraryEventService libraryEventService;

  @KafkaListener(topics = "${kafka-topic}")
  public void onMessage(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
    log.info("LibraryEventConsumer Received consumer record form BATCH ACK Mode consumer {}", consumerRecord);
    libraryEventService.processLibraryEvent(consumerRecord);
  }
}
