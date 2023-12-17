package com.springtrial.service;

import com.springtrial.entity.FailureRecord;
import com.springtrial.repository.FailureRecordRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class FailureRecordService {

  @Autowired
  FailureRecordRepository failureRecordRepository;

  public void saveFailedRecord(ConsumerRecord<Integer, String> consumerRecord, Exception e, String status) {
    FailureRecord failureRecord = new FailureRecord(null, consumerRecord.topic(), consumerRecord.key(), consumerRecord.value(), consumerRecord.partition(), consumerRecord.offset(), e.getCause().getClass().getName(), status);
    failureRecordRepository.save(failureRecord);
  }
}
