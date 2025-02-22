package com.github.nlayna.ksolar.config;

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.Optional;

@Configuration
public class SolrConfig {

    @Bean
    public CloudSolrClient cloudSolrClient(@Value("${solr.zk-hosts}") String zkHosts,
                                           @Value("${solr.zk-chroot}") String zkChroot,
                                           @Value("${solr.collection}") String collection) {
        return new CloudSolrClient.Builder(Arrays.asList(zkHosts.split(",")), Optional.of(zkChroot))
                .withDefaultCollection(collection)
                .build();
    }
}
