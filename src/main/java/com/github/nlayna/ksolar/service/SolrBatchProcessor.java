package com.github.nlayna.ksolar.service;


import com.github.nlayna.ksolar.model.SolrMessage;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class SolrBatchProcessor {
    private final CloudSolrClient solrClient;
    private final int batchSize;
    private final List<SolrMessage> batchQueue;
    private final ScheduledExecutorService scheduler;

    private final AtomicLong totalProcessedDocuments;
    private final AtomicLong totalFailedBatches;
    private final Timer batchLatencyTimer;
    private final Timer documentLatencyTimer;

    public SolrBatchProcessor(CloudSolrClient solrClient,
                              @Value("${solr.batch-size}") int batchSize,
                              @Value("${solr.flush-interval-ms}") long flushIntervalMs,
                              MeterRegistry registry) {
        this.solrClient = solrClient;
        this.batchSize = batchSize;
        this.batchQueue = Collections.synchronizedList(new ArrayList<>());
        this.scheduler = Executors.newScheduledThreadPool(1);

        this.totalProcessedDocuments = registry.gauge("solr.total_document", new AtomicLong(0));
        this.totalFailedBatches = registry.gauge("solr.failed_batches", new AtomicLong(0));
        this.batchLatencyTimer = registry.timer("solr.batch_latency");
        this.documentLatencyTimer = registry.timer("solr.document_latency");

        this.scheduler.scheduleAtFixedRate(this::flushBatch, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);
    }

    public synchronized  void addToBatch(SolrMessage message) {
        batchQueue.add(message);
        if (batchQueue.size() >= batchSize) {
            flushBatch();
        }
    }

    private synchronized void flushBatch() {
        if (batchQueue.isEmpty()) {
            return;
        }
        long batchStartTime = System.currentTimeMillis();

        try {
            List<SolrInputDocument> documents = new ArrayList<>();

            for (SolrMessage msg : batchQueue) {
                if ("I".equals(msg.getAction()) || "U".equals(msg.getAction())) {
                    documents.add(msg.toSolrInputDocument());

                    var docLatency = System.currentTimeMillis() - msg.getKafkaTimestamp();
                    documentLatencyTimer.record(docLatency, TimeUnit.MILLISECONDS);
                } else if ("D".equals(msg.getAction())) {
                    solrClient.deleteById(msg.getId());
                }
            }

            if (!documents.isEmpty()) {
                solrClient.add(documents);
            }
            solrClient.commit();
            batchQueue.clear();

            batchLatencyTimer.record(System.currentTimeMillis() - batchStartTime, TimeUnit.MILLISECONDS);
            totalProcessedDocuments.addAndGet(documents.size());
        } catch (SolrServerException | IOException e) {
            totalFailedBatches.incrementAndGet();
            log.error("Error while flushing batch", e);
        }
    }
}
