/**
 * 
 */
package com.bigdatafly.factory;

import jersey.repackaged.com.google.common.base.Preconditions;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;

/**
 * @author summer
 *
 */
public class ElasticSearchClientSingleFactory implements Factory<Client>{

	/**
	 * 
	 */
	
	private static volatile ElasticSearchClientSingleFactory factory;
	
	private Settings settings;
	
	private ElasticSearchClientSingleFactory() {
		
	}

	public TransportClient create() {
		
		TransportClient client = new TransportClient.Builder()
		.settings(settings)
		.build();
		
		return client;
	}

	public static ElasticSearchClientSingleFactory builder() {
		
		ElasticSearchClientSingleFactory ins = factory;
		if(ins == null){
			synchronized(ElasticSearchClientSingleFactory.class){
				if(ins == null){
					ins = new ElasticSearchClientSingleFactory();
				}
				
				factory = ins;
			}
		}
		return factory;
	}
	
	public ElasticSearchClientSingleFactory settings(Settings settings){
		
		Preconditions.checkArgument(settings != null,"elasticsearch configurations is null");
		this.settings = settings;
		
		return this;
	}

}
