/**
 * 
 */
package com.bigdatafly.elasticsearch;

import java.util.List;
import java.util.Map;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdatafly.factory.ElasticSearchClientSingleFactory;


/**
 * @author summer
 *
 */
public class ElasticSearchClient {

	private static final Logger logger = LoggerFactory.getLogger(ElasticSearchClient.class);
	
	private ElasticSearchConfiguration configurations;
	private TransportClient client;
	/**
	 * 
	 */
	public ElasticSearchClient(ElasticSearchConfiguration configurations) {
		
		this.configurations = configurations;
	}

	private TransportAddress[] getNodesTransportAddress(ElasticSearchConfiguration configurations){
		
		return configurations.getNodesTransportAddress();
	}
	
	private TransportClient getTransportClient(){
		
		Settings settings = Settings
				.settingsBuilder()
				.put(ElasticSearchConstants.CLUSTER_NAME_KEY,
						configurations.getString(ElasticSearchConstants.CLUSTER_NAME_KEY,
								ElasticSearchConstants.DEFAULT_NAME_SEVR))
				.build();
		TransportAddress[] transportAddress = getNodesTransportAddress(configurations);
		
		return ElasticSearchClientSingleFactory
				.builder()
				.settings(settings)
				.create()
				.addTransportAddresses(transportAddress);
	}
}
