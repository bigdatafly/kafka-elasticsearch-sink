/**
 * 
 */
package com.bigdatafly.elasticsearch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.sink.SinkRecord;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;




/**
 * @author summer
 *
 */
public class ElasticSearchClient {

	private static final Logger logger = LoggerFactory.getLogger(ElasticSearchClient.class);
	
	private ElasticSearchConfiguration configurations;
	private ElasticSearchSingletonClient client;
	private ElasticSearchSingletonBulkProcessor processor;
	/**
	 * 
	 */
	public ElasticSearchClient(ElasticSearchConfiguration configurations) {
		
		this.configurations = configurations;
	}


	public void start() {
		
		Settings settings = Settings
				.settingsBuilder()
				.put(ElasticSearchConstants.CLUSTER_NAME_KEY,
						configurations.getString(ElasticSearchConstants.CLUSTER_NAME_KEY,
								ElasticSearchConstants.DEFAULT_NAME_SEVR))
				.build();
		TransportAddress[] transportAddress = getNodesTransportAddress(configurations);
		
		client = ElasticSearchSingletonClient
				.builder()
				.settings(settings)
				.addTransportAddresses(transportAddress)
				.create();
		processor = new ElasticSearchSingletonBulkProcessor(client);
		
		logger.debug("start {}");
	}
	
	private TransportAddress[] getNodesTransportAddress(ElasticSearchConfiguration configurations){
		
		return configurations.getNodesTransportAddress();
	}

	public void shutdown() {
		
		waitUntilNonePending();
		processor.close();
		client.close();
		
		logger.debug("shutdown {}");
	}

	public void waitUntilNonePending() {
		
		processor.waitUntilNonePending();
	}

	public void updatePendingRequests(int count) {
		
		processor.updatePendingRequests(count);
	}

	public boolean process(byte[] record) {
		
		
		boolean success = false;
		
        if (record != null) {
            processor.add(configurations.getIndexName(),configurations.getTypeName(),record);
            success = true;
        }
        return success;
		
		
	}

	
	
}
