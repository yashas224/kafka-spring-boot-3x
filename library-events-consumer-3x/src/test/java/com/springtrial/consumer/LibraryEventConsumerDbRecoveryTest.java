package com.springtrial.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.springtrial.entity.Book;
import com.springtrial.entity.LibraryEvent;
import com.springtrial.entity.LibraryEventType;
import com.springtrial.repository.FailureRecordRepository;
import com.springtrial.service.LibraryEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@SpringBootTest
@EmbeddedKafka(topics = {"${kafka-topic}"}, partitions = 3)
@TestPropertySource(properties = {"manual-commit=false", "kafka-bootstrap-servers=${spring.embedded.kafka.brokers}",
   "recovery-DB-save=true"})
public class LibraryEventConsumerDbRecoveryTest {
  @Value("${kafka-topic}")
  private String kafkaTopic;

  @Autowired
  private EmbeddedKafkaBroker embeddedKafka;
  @Autowired
  KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;
  @Autowired
  ObjectMapper objectMapper;

  @SpyBean
  LibraryEventService libraryEventService;

  @SpyBean
  LibraryEventConsumer libraryEventConsumer;

  @SpyBean
  FailureRecordRepository failureRecordRepository;
  KafkaTemplate<Integer, String> kafkaTemplate;

  @BeforeEach
  void initProducerAndConsumers() {

    // only waiting for containers with group id = library-event-consumer-3x-app
    // retry listener container listeners are disabled - retryListener-autostart=false
    var libraryEventConsumerListners = kafkaListenerEndpointRegistry.getListenerContainers().stream()
       .filter(messageListenerContainer -> Objects.equals(messageListenerContainer.getGroupId(), "library-event-consumer-3x-app"))
       .collect(Collectors.toList());

    for(MessageListenerContainer container : libraryEventConsumerListners) {
      ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
    }

    var producerConfig = KafkaTestUtils.producerProps(embeddedKafka);
    var producerFactory = new DefaultKafkaProducerFactory<Integer, String>(producerConfig);
    kafkaTemplate = new KafkaTemplate<>(producerFactory);
  }

  @AfterEach
  void tearDown() {
    failureRecordRepository.deleteAll();
  }

  @Test
  void testPublishUpdateLibraryEventExceptionCase_null_eventId()
     throws JsonProcessingException, ExecutionException, InterruptedException {
    LibraryEvent libraryEvent = LibraryEvent.builder()
       .libraryEventId(null).libraryEventType(LibraryEventType.NEW).book(
          Book.builder().bookId(456).bookAuthor("Virat").bookName("Life in India").build())
       .build();

    // publish update library Event
    var updatedBookName = "Cricket in depth";
    String updatedBookAuthor = "Virat Kohli";
    libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
    String updateLibraryEventString = objectMapper.writeValueAsString(libraryEvent);
    kafkaTemplate.send(kafkaTopic, libraryEvent.getLibraryEventId(), updateLibraryEventString).get();

    Thread.sleep(5000);

    Mockito.verify(libraryEventConsumer, Mockito.times(1)).onMessage(Mockito.any(ConsumerRecord.class));
    Mockito.verify(libraryEventService, Mockito.times(1)).processLibraryEvent(Mockito.any(ConsumerRecord.class));

    //  DB verification
    var count = failureRecordRepository.count();
    Assertions.assertEquals(1, count);
    var record = failureRecordRepository.findAll().get(0);
    System.out.println("DB record ".concat(record.toString()));
    Assertions.assertEquals("DEAD", record.getStatus());
  }
}
