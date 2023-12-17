package com.springtrial.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.springtrial.producer.LibraryEventProducer;
import com.springtrial.util.TestUtil;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.net.URI;
import java.util.concurrent.CompletableFuture;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = LibraryEventController.class)
public class LibraryEventControllerUnitTest {

  @Autowired
  ObjectMapper objectMapper;
  @MockBean
  LibraryEventProducer libraryEventProducer;
  @Autowired
  MockMvc mockMvc;

  @Test
  void testPostLibraryEvent() throws Exception {
    var requestBody = TestUtil.libraryEventRecord();
    Mockito.when(libraryEventProducer.sendLibraryEvent(requestBody)).thenReturn(new CompletableFuture<>());
    mockMvc.perform(post(URI.create("/v1/libraryevent"))
          .contentType(MediaType.APPLICATION_JSON)
          .content(objectMapper.writeValueAsString(requestBody)))
       .andExpect(status().isCreated());
  }

  @Test
  void testPostLibraryEvent_invalid_4xx() throws Exception {
    var requestBody = TestUtil.libraryEventRecordWithInvalidBook();
    Mockito.when(libraryEventProducer.sendLibraryEvent(requestBody)).thenReturn(new CompletableFuture<>());
    mockMvc.perform(post(URI.create("/v1/libraryevent"))
          .contentType(MediaType.APPLICATION_JSON)
          .content(objectMapper.writeValueAsString(requestBody)))
       .andExpect(status().is4xxClientError());
  }
}
