package com.github.nlayna.ksolar.service;

import com.github.nlayna.ksolar.model.SolrMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaConsumerService {
    private final SolrBatchProcessor solrBatchProcessor;

    @KafkaListener(topics = "${kafka.topic}", groupId = "ksolar-loader-group")
    public void consume(ConsumerRecord<String, String> record) {
        try {
            var msg = SolrMessage.fromJson(record.value());
            msg.setKafkaTimestamp(record.timestamp());
            solrBatchProcessor.addToBatch(msg);
        } catch (Exception e) {
            log.error("Error while processing record", e);
        }
    }
}
