package com.springtrial.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.springtrial.entity.Book;
import com.springtrial.entity.LibraryEvent;
import com.springtrial.entity.LibraryEventType;
import com.springtrial.exception.RecoverableException;
import com.springtrial.repository.LibraryEventRepository;
import com.springtrial.service.LibraryEventService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@SpringBootTest
@EmbeddedKafka(topics = {"${kafka-topic}", "${kafka-retry-topic}", "${kafka-dlt-topic}"}, partitions = 3)
@TestPropertySource(properties = {"manual-commit=false", "kafka-bootstrap-servers=${spring.embedded.kafka.brokers}",
   "retryListener-autostart=false", "recovery-DB-save=false"})
public class LibraryEventConsumerTest {

  @Value("${kafka-topic}")
  private String kafkaTopic;
  @Value("${kafka-retry-topic}")
  private String retryTopic;

  @Value("${kafka-dlt-topic}")
  private String deadLetterTopic;
  @Value("${retryListener-autostart}")
  private boolean retryListenerAutoStartUp;
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
  LibraryEventRepository libraryEventRepository;
  KafkaTemplate<Integer, String> kafkaTemplate;

  @BeforeEach
  void initProducerAndConsumers() {
    if(retryListenerAutoStartUp) {
      // wait till all the  consumer listeners  are initialized completely
      for(MessageListenerContainer listenerContainer : kafkaListenerEndpointRegistry.getListenerContainers()) {
        ContainerTestUtils.waitForAssignment(listenerContainer, embeddedKafka.getPartitionsPerTopic());
      }
    } else {
      // only waiting for containers with group id = library-event-consumer-3x-app
      // retry listener container listeners are disabled - retryListener-autostart=false
      var libraryEventConsumerListners = kafkaListenerEndpointRegistry.getListenerContainers().stream()
         .filter(messageListenerContainer -> Objects.equals(messageListenerContainer.getGroupId(), "library-event-consumer-3x-app"))
         .collect(Collectors.toList());

      for(MessageListenerContainer container : libraryEventConsumerListners) {
        ContainerTestUtils.waitForAssignment(container, embeddedKafka.getPartitionsPerTopic());
      }
    }

    var producerConfig = KafkaTestUtils.producerProps(embeddedKafka);
    var producerFactory = new DefaultKafkaProducerFactory<Integer, String>(producerConfig);
    kafkaTemplate = new KafkaTemplate<>(producerFactory);
  }

  @AfterEach
  void tearDown() {
    libraryEventRepository.deleteAll();
  }

  @Test
  void testPublishNewLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
    LibraryEvent libraryEvent = LibraryEvent.builder()
       .libraryEventId(null).libraryEventType(LibraryEventType.NEW).book(
          Book.builder().bookId(456).bookAuthor("yashas samaga").bookName("Life of an Engineer").build())
       .build();
    String libraryEventStr = objectMapper.writeValueAsString(libraryEvent);
    kafkaTemplate.send(kafkaTopic, libraryEvent.getLibraryEventId(), libraryEventStr).get();

    Thread.sleep(3000);
    // capturing consumer record consumed in LibraryEventConsumer
    ArgumentCaptor<ConsumerRecord> consumerRecordArgumentCaptor = ArgumentCaptor.forClass(ConsumerRecord.class);

    Mockito.verify(libraryEventConsumer).onMessage(consumerRecordArgumentCaptor.capture());

    ConsumerRecord<Integer, String> consumerRecord = consumerRecordArgumentCaptor.getValue();
    Assertions.assertEquals(libraryEventStr, consumerRecord.value());

    Mockito.verify(libraryEventService).processLibraryEvent(consumerRecord);

    // DB verification

    LibraryEvent dbLibraryEvent = libraryEventRepository.findAll().get(0);
    Assertions.assertEquals(libraryEvent.getBook().getBookName(), dbLibraryEvent.getBook().getBookName());
  }

  @Test
  void testPublishUpdateLibraryEvent() throws JsonProcessingException, ExecutionException, InterruptedException {
    // save Library event
    LibraryEvent libraryEvent = LibraryEvent.builder()
       .libraryEventId(null).libraryEventType(LibraryEventType.NEW).book(
          Book.builder().bookId(456).bookAuthor("Virat").bookName("Life in India").build())
       .build();
    libraryEvent.getBook().setLibraryEvent(libraryEvent);
    libraryEvent = libraryEventRepository.save(libraryEvent);

    // publish update library Event
    libraryEvent.getBook().setLibraryEvent(null);
    var updatedBookName = "Cricket in depth";
    String updatedBookAuthor = "Virat Kohli";
    libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
    libraryEvent.getBook().setBookAuthor(updatedBookAuthor);
    libraryEvent.getBook().setBookName(updatedBookName);
    String updateLibraryEventString = objectMapper.writeValueAsString(libraryEvent);
    kafkaTemplate.send(kafkaTopic, libraryEvent.getLibraryEventId(), updateLibraryEventString).get();

    Thread.sleep(3000);

    ArgumentCaptor<ConsumerRecord> consumerRecordArgumentCaptor = ArgumentCaptor.forClass(ConsumerRecord.class);
    Mockito.verify(libraryEventConsumer).onMessage(consumerRecordArgumentCaptor.capture());
    ConsumerRecord<Integer, String> consumerRecord = consumerRecordArgumentCaptor.getValue();
    Assertions.assertEquals(updateLibraryEventString, consumerRecord.value());

    Mockito.verify(libraryEventService).processLibraryEvent(consumerRecord);

    // DB verification

    Optional<LibraryEvent> optionalLibraryEventDb = libraryEventRepository.findById(libraryEvent.getLibraryEventId());
    var dbLibraryEvent = optionalLibraryEventDb.get();
    Assertions.assertEquals(LibraryEventType.UPDATE, dbLibraryEvent.getLibraryEventType());
    Assertions.assertEquals(updatedBookName, dbLibraryEvent.getBook().getBookName());
    Assertions.assertEquals(updatedBookAuthor, dbLibraryEvent.getBook().getBookAuthor());
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

    // No DB call
    Mockito.verifyNoInteractions(libraryEventRepository);

    // init consumer
    Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testT1", "false", embeddedKafka);
    DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(
       consumerProps);
    Consumer<Integer, String> kafkaConsumer = cf.createConsumer();
    embeddedKafka.consumeFromAnEmbeddedTopic(kafkaConsumer, deadLetterTopic);

    var record = KafkaTestUtils.getSingleRecord(kafkaConsumer, deadLetterTopic);
    System.out.println("Record topic ".concat(record.topic()));
    System.out.println("Record Value ".concat(record.value()));
    Assertions.assertEquals(updateLibraryEventString, record.value());
  }

  @Test
  void testPublishNewLibraryEvent_DB_Exception() throws JsonProcessingException, ExecutionException, InterruptedException {

    LibraryEvent libraryEvent = LibraryEvent.builder()
       .libraryEventId(null).libraryEventType(LibraryEventType.NEW).book(
          Book.builder().bookId(456).bookAuthor("yashas samaga").bookName("Life of an Engineer").build())
       .build();

    // throwing DB exception when message is saved so that teh retry is triggered
    Mockito.doThrow(new RecoverableException("Not able to connect tp DB server"))
       .when(libraryEventRepository).save(Mockito.any(LibraryEvent.class));

    String libraryEventStr = objectMapper.writeValueAsString(libraryEvent);
    kafkaTemplate.send(kafkaTopic, libraryEvent.getLibraryEventId(), libraryEventStr).get();

    Thread.sleep(5000);

    // verifying retries
    Mockito.verify(libraryEventConsumer, Mockito.times(3)).onMessage(Mockito.any(ConsumerRecord.class));
    Mockito.verify(libraryEventService, Mockito.times(3)).processLibraryEvent(Mockito.any(ConsumerRecord.class));

    // init consumer
    Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("testT", "false", embeddedKafka);
    DefaultKafkaConsumerFactory<Integer, String> cf = new DefaultKafkaConsumerFactory<Integer, String>(
       consumerProps);
    Consumer<Integer, String> kafkaConsumer = cf.createConsumer();
    embeddedKafka.consumeFromAnEmbeddedTopic(kafkaConsumer, retryTopic);

    var record = KafkaTestUtils.getSingleRecord(kafkaConsumer, retryTopic);
    System.out.println("Record topic ".concat(record.topic()));
    System.out.println("Record Value ".concat(record.value()));
    Assertions.assertEquals(libraryEventStr, record.value());
  }
}
