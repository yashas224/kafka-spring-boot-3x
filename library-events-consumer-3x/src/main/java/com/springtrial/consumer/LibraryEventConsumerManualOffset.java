package com.springtrial.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@ConditionalOnProperty(name = "manual-commit", havingValue = "true")
@Component
@Slf4j
public class LibraryEventConsumerManualOffset {

  @Value("${ack-flag}")
  private boolean ackFlag;

  @KafkaListener(topics = "${kafka-topic}")
  public void onMessage(ConsumerRecord<Integer, String> consumerRecord, Acknowledgment acknowledgment) {
    log.info("LibraryEventConsumerManualOffset Received consumer record from MANUAL ACK MODE consumer  {}", consumerRecord);
    if(ackFlag) {
      log.info("acknowledging received message ................");
      acknowledgment.acknowledge();
    }
  }
}
