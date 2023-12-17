package com.springtrial.scheduler;

import com.springtrial.entity.FailureRecord;
import com.springtrial.repository.FailureRecordRepository;
import com.springtrial.service.LibraryEventService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@ConditionalOnProperty(name = "scheduler-enabled", havingValue = "true")
public class RetryScheduler {

  @Autowired
  FailureRecordRepository failureRecordRepository;

  @Autowired
  LibraryEventService libraryEventService;

  @Scheduled(fixedRate = 10000)
  public void retryFailedRecord() {
    log.info("Scheduler started");
    failureRecordRepository.findByStatus("RETRY")
       .forEach(failureRecord -> {
         var consumerRecord = getConsumerRecord(failureRecord);
         log.info("Retrying failed record {}", failureRecord);
         try {
           libraryEventService.processLibraryEvent(consumerRecord);
           failureRecord.setStatus("SUCCESS");
           failureRecordRepository.saveAndFlush(failureRecord);
         } catch(Exception e) {
           log.error("Exception in RetryScheduler ", e);
         }
       });
    log.info("Scheduler completed");
  }

  private ConsumerRecord<Integer, String> getConsumerRecord(FailureRecord failureRecord) {
    return new ConsumerRecord<>(failureRecord.getTopic(), failureRecord.getPartition(),
       failureRecord.getOffsetValue(), failureRecord.getKeyValue(), failureRecord.getErrorRecord());
  }
}

