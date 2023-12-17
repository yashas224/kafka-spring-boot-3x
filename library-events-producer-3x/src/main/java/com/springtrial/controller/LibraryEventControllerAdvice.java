package com.springtrial.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.util.stream.Collectors;

@ControllerAdvice
@Slf4j
public class LibraryEventControllerAdvice {

  @ExceptionHandler(MethodArgumentNotValidException.class)
  public ResponseEntity<String> handleException(MethodArgumentNotValidException exception) {
    var message = exception.getBindingResult()
       .getFieldErrors()
       .stream().map(fieldError -> fieldError.getField() + "-" + fieldError.getDefaultMessage())
       .collect(Collectors.joining(","));
    log.info("error message {}", message);
    return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(message);
  }
}
