package com.springtrial.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfiguration {

  @Value("${kafka-bootstrap-servers}")
  private String bootstrapServer;

  @Value("${kafka-topic}")
  private String kafkaTopic;

  public DefaultKafkaProducerFactory<Integer, String> kafkaProducerFactory() {
    return new DefaultKafkaProducerFactory<>(buildProducerProperties());
  }

  private Map<String, Object> buildProducerProperties() {
    Map<String, Object> map = new HashMap<>();
    map.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    map.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
    map.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//    map.put(ProducerConfig.ACKS_CONFIG, 1);
    map.put(ProducerConfig.RETRIES_CONFIG, 10);
    return map;
  }

  @Bean
  public KafkaTemplate<Integer, String> kafkaTemplate() {
    return new KafkaTemplate<>(kafkaProducerFactory());
  }

  @Bean
  public KafkaAdmin admin() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    return new KafkaAdmin(configs);
  }

  @Bean
  public NewTopic topic1() {
    return TopicBuilder.name(kafkaTopic)
       .partitions(3)
       .replicas(1)
       .build();
  }
}
