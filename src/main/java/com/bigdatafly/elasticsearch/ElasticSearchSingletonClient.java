/**
 * 
 */
package com.bigdatafly.elasticsearch;

import java.util.function.Supplier;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;

import com.bigdatafly.factory.SingletonFactory;
import com.google.common.base.Preconditions;

/**
 * @author summer
 *
 */
public class ElasticSearchSingletonClient{

	/**
	 * 
	 */
	private Settings settings;
	private TransportClient client;
	private TransportAddress[] transportAddress;
	
	private ElasticSearchSingletonClient() {
		
	}

	/**
	 * lambda 就是NB
	 * @return
	 */
	public ElasticSearchSingletonClient create() {
		
		Preconditions.checkArgument(settings != null,
				"elasticsearch configurations is null");
		client = SingletonFactory.of(new Supplier<TransportClient>(){
			@Override
			public TransportClient get() {
				return new TransportClient.Builder()
							.settings(settings)
							.build();
			}
		}).get();
		if(transportAddress!=null)
			client.addTransportAddresses(transportAddress);
		return this;
	}

	public ElasticSearchSingletonClient addTransportAddresses(TransportAddress[] transportAddress){
		
		this.transportAddress = transportAddress;
		return this;
	}
	
	public static ElasticSearchSingletonClient builder() {
		
		return new ElasticSearchSingletonClient();
	}
	
	public ElasticSearchSingletonClient settings(Settings settings){
		
		this.settings = settings;
		return this;
	}

	public Settings getSettings() {
		return settings;
	}

	public void setSettings(Settings settings) {
		this.settings = settings;
	}

	public TransportClient getClient() {
		return client;
	}

	public void setClient(TransportClient client) {
		this.client = client;
	}

	public TransportAddress[] getTransportAddress() {
		return transportAddress;
	}

	public void setTransportAddress(TransportAddress[] transportAddress) {
		this.transportAddress = transportAddress;
	}

	public void close() {
		
		client.close();
		
	}
	
	

}
