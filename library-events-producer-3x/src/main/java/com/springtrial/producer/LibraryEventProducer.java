package com.springtrial.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.springtrial.domain.LibraryEvent;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class LibraryEventProducer {
  @Value("${kafka-topic}")
  private String kafkaTopic;
  @Autowired
  private KafkaTemplate<Integer, String> kafkaTemplate;

  @Autowired
  ObjectMapper objectMapper;

  // blocking call to get all metadata for the first time
  @SneakyThrows
  public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent(LibraryEvent libraryEvent) {
    Integer key = libraryEvent.libraryEventId();
    String value = objectMapper.writeValueAsString(libraryEvent);
    var future = kafkaTemplate.send(kafkaTopic, key, value);
    return future.whenComplete((sendResult, throwable) -> {
      if(throwable != null) {
        handleFailure(key, value, throwable);
      } else {
        handleSuccess(key, value, sendResult);
      }
    });
  }

  private void handleSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
    log.info("successfully sent message to kafka key {} value {} partition {} ", key, value, sendResult.getRecordMetadata().partition());
  }

  private void handleFailure(Integer key, String value, Throwable throwable) {
    log.error("error while sending message to kafka", throwable);
  }

  // blocking call to get all metadata for the first time
  @SneakyThrows
  public void sendLibraryEventSync(LibraryEvent libraryEvent) {
    Integer key = libraryEvent.libraryEventId();
    String value = objectMapper.writeValueAsString(libraryEvent);
    var result = kafkaTemplate.send(kafkaTopic, key, value).get(3, TimeUnit.SECONDS);
    handleSuccess(key, value, result);
  }

  @SneakyThrows
  public void sendLibraryEventProducerRecord(LibraryEvent libraryEvent) {
    Integer key = libraryEvent.libraryEventId();
    String value = objectMapper.writeValueAsString(libraryEvent);

    ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(kafkaTopic, null, key, value, Arrays.asList(new RecordHeader("event-source", "library-scanner".getBytes())));
    var result = kafkaTemplate.send(producerRecord).get(3, TimeUnit.SECONDS);
    handleSuccess(key, value, result);
  }
}
