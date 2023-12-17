package com.springtrial;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class LibraryEventsConsumer3xApplication {

  public static void main(String[] args) {
    SpringApplication.run(LibraryEventsConsumer3xApplication.class, args);
  }
}
