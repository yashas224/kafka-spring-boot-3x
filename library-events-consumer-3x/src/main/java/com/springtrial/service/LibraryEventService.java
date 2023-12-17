package com.springtrial.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.springtrial.entity.LibraryEvent;
import com.springtrial.exception.RecoverableException;
import com.springtrial.repository.LibraryEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class LibraryEventService {

  @Autowired
  LibraryEventRepository libraryEventRepository;

  @Autowired
  ObjectMapper objectMapper;

  public void processLibraryEvent(ConsumerRecord<Integer, String> consumerRecord) throws JsonProcessingException {
    LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
    switch(libraryEvent.getLibraryEventType()) {
      case NEW:
        save(libraryEvent);
        break;
      case UPDATE:
        validate(libraryEvent);
        save(libraryEvent);
        break;
      default:
        log.info("invalid type received");
    }
  }

  private void validate(LibraryEvent libraryEvent) {
    if(libraryEvent.getLibraryEventId() == null) {
      throw new IllegalArgumentException("Null event ID");
    }
    Boolean exists = libraryEventRepository.existsById(libraryEvent.getLibraryEventId());
    if(!exists) {
      throw new IllegalArgumentException("event ID doesn't exists");
    }
    log.info("Validation successful");
  }

  private void save(LibraryEvent libraryEvent) {
    // test logic
    if(libraryEvent.getLibraryEventId() != null && libraryEvent.getLibraryEventId() == 69) {
      throw new RecoverableException("DB failure");
    }
    libraryEvent.getBook().setLibraryEvent(libraryEvent);
    var savedEntity = libraryEventRepository.save(libraryEvent);
    log.info("Saving Entity :{}", savedEntity);
  }
}
