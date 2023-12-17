package com.springtrial.config;

import com.springtrial.exception.RecoverableException;
import com.springtrial.service.FailureRecordService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@Slf4j
public class KafkaConsumerConfig {

  @Value("${kafka-bootstrap-servers}")
  private String bootstrapServer;

  @Value("${manual-commit}")
  private boolean manualCommitOffset;

  @Value("${concurrency-level:1}")
  private int concurrencyLevel;

  @Value("${kafka-retry-topic}")
  private String retryTopic;

  @Value("${kafka-dlt-topic}")
  private String deadLetterTopic;

  @Value("${recovery-DB-save}")
  private boolean recoveryDBSave;

  @Autowired
  FailureRecordService failureRecordService;

  @Bean
  public ConsumerFactory<Integer, String> consumerFactory() {
    return new DefaultKafkaConsumerFactory<>(consumerConfigs());
  }

  @Bean
  public Map<String, Object> consumerConfigs() {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "library-event-consumer-3x-app");
    return props;
  }

  @Bean
  KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Integer, String>>
  kafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
       new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory());
    if(manualCommitOffset) {
      log.info("setting AckMode to  MANUAL");
      factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
    } else {
      log.info("setting AckMode to  Default BATCH");
    }
    factory.setConcurrency(concurrencyLevel);
    // error handlers
    factory.setCommonErrorHandler(getErrorHandler());
    return factory;
  }

  public DefaultErrorHandler getErrorHandler() {
    FixedBackOff fixedBackOff = new FixedBackOff(1000L, 2);

    ExponentialBackOffWithMaxRetries exponentialBack = new ExponentialBackOffWithMaxRetries(2);
    exponentialBack.setInitialInterval(1_000L);
    exponentialBack.setMultiplier(2.0);
    DefaultErrorHandler defaultErrorHandler = null;
    if(recoveryDBSave) {
      // save to DB  -- recovery mechanism
      log.info("recovery mechanism - save to DB ");
      defaultErrorHandler = new DefaultErrorHandler(DbconsumerRecordRecoverer, fixedBackOff);
    } else {
      log.info("recovery mechanism - publish to retry/DLT topics ");
      // publish to retry OR DLT topics -- recovery mechanism
      defaultErrorHandler = new DefaultErrorHandler(recoverer(), exponentialBack);
    }
    // listen each retry attempt
    defaultErrorHandler.setRetryListeners(((record, ex, deliveryAttempt) -> {
      log.info("Failed record in Retry Listener , exception {}, delivery attempt {} ", ex, deliveryAttempt);
    }));
    // ignore exceptions
    defaultErrorHandler.addNotRetryableExceptions(IllegalArgumentException.class);

    return defaultErrorHandler;
  }

  ConsumerRecordRecoverer DbconsumerRecordRecoverer = (consumerRecord, e) -> {
    log.info("inside DB Recoverer logic ");
    log.info("Exception class  thrown by Listener {}", e.getCause().getClass());
    var record = (ConsumerRecord<Integer, String>) consumerRecord;
    if(e.getCause() instanceof RecoverableException) {
      // recovery  logic
      log.info("inside Recovery logic ");
      failureRecordService.saveFailedRecord(record, e, "RETRY");
    } else {
      // non recovery logic
      log.info("inside non Recovery logic ");
      failureRecordService.saveFailedRecord(record, e, "DEAD");
    }
  };

  @Bean
  public ProducerFactory<Integer, String> producerFactory() {
    return new DefaultKafkaProducerFactory<>(producerConfigs());
  }

  @Bean
  public Map<String, Object> producerConfigs() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    return props;
  }

  @Bean
  public KafkaTemplate<Integer, String> kafkaTemplate() {
    return new KafkaTemplate<>(producerFactory());
  }

  public DeadLetterPublishingRecoverer recoverer() {
    DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate(),
       (r, e) -> {
         log.info("inside Recoverer logic ");
         log.info("Exception class  thrown by Listener {}", e.getCause().getClass());
         if(e.getCause() instanceof RecoverableException) {
           return new TopicPartition(retryTopic, r.partition());
         } else {
           return new TopicPartition(deadLetterTopic, r.partition());
         }
       });

    return recoverer;
  }
}
