package com.springtrial.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.springtrial.domain.LibraryEvent;
import com.springtrial.util.TestUtil;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.Map;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
// starting embedded kafka broker
@EmbeddedKafka(topics = "${kafka-topic}")
@TestPropertySource(properties = {"kafka-bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventControllerIntegrationTest {
  @Autowired
  TestRestTemplate testRestTemplate;
  @Autowired
  private EmbeddedKafkaBroker kafkaEmbedded;

  private Consumer<Integer, String> consumer;

  @Autowired
  ObjectMapper objectMapper;
  @Value("${kafka-topic}")
  private String topic;

  @BeforeEach
  void initConsumer() {
    Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testGroup", "true", kafkaEmbedded);
    DefaultKafkaConsumerFactory<Integer, String> consumerFactory = new DefaultKafkaConsumerFactory<>
       (consumerProps, new IntegerDeserializer(), new StringDeserializer());
    consumer = consumerFactory.createConsumer();
    kafkaEmbedded.consumeFromAnEmbeddedTopic(consumer, topic);
  }

  @AfterEach
  void destroyConsumer() {
    consumer.close();
  }

  @Test
  void testPostLibraryEvent() throws JsonProcessingException {
    var request = TestUtil.libraryEventRecord();
    HttpHeaders httpHeaders = new HttpHeaders();
    httpHeaders.add(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE);

    var httpEntity = new HttpEntity<>(request, httpHeaders);

    ResponseEntity<LibraryEvent> responseEntity = testRestTemplate
       .exchange("/v1/libraryevent/sync", HttpMethod.POST, httpEntity, LibraryEvent.class);

    LibraryEvent responseBody = responseEntity.getBody();
    Assertions.assertEquals(HttpStatus.CREATED.value(), responseEntity.getStatusCode().value());
    Assertions.assertEquals(request, responseBody);

    ConsumerRecord<Integer, String> consumerRecord = KafkaTestUtils.getSingleRecord(consumer, topic);
    LibraryEvent kafkaLibraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
    Assertions.assertEquals(request, kafkaLibraryEvent);
  }
}


