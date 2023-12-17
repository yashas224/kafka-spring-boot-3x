package com.springtrial.controller;

import com.springtrial.domain.LibraryEvent;
import com.springtrial.domain.LibraryEventType;
import com.springtrial.producer.LibraryEventProducer;
import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LibraryEventController {

  @Autowired
  LibraryEventProducer libraryEventProducer;

  @PostMapping("/v1/libraryevent")
  public ResponseEntity<LibraryEvent> postLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) {
    log.info("received  libraryEvent {}", libraryEvent);
    libraryEventProducer.sendLibraryEvent(libraryEvent);
    return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
  }

  @PostMapping("/v1/libraryevent/sync")
  public ResponseEntity<LibraryEvent> postLibraryEventSync(@RequestBody LibraryEvent libraryEvent) {
    log.info("received  libraryEvent {}", libraryEvent);
    libraryEventProducer.sendLibraryEventSync(libraryEvent);
    return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
  }

  @PostMapping("/v1/libraryevent/header")
  public ResponseEntity<LibraryEvent> postLibraryEventHeader(@RequestBody LibraryEvent libraryEvent) {
    log.info("received  libraryEvent {}", libraryEvent);
    libraryEventProducer.sendLibraryEventProducerRecord(libraryEvent);
    return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
  }

  @PutMapping("/v1/libraryevent")
  public ResponseEntity<?> updateLibraryEvent(@RequestBody @Valid LibraryEvent libraryEvent) {
    log.info("received  libraryEvent {}", libraryEvent);
    ResponseEntity<String> BAD_REQUEST = validateRequest(libraryEvent);
    if(BAD_REQUEST != null) return BAD_REQUEST;
    libraryEventProducer.sendLibraryEventProducerRecord(libraryEvent);
    return ResponseEntity.status(HttpStatus.OK).body(libraryEvent);
  }

  private static ResponseEntity<String> validateRequest(LibraryEvent libraryEvent) {
    if(libraryEvent.libraryEventId() == null) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Pass the Library event Id");
    }
    if(!libraryEvent.libraryEventType().equals(LibraryEventType.UPDATE)) {
      return ResponseEntity.status(HttpStatus.BAD_REQUEST).body("Non update event type received ");
    }
    return null;
  }
}
