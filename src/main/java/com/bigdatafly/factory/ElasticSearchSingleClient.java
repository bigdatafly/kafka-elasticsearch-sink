/**
 * 
 */
package com.bigdatafly.factory;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;

import com.google.common.base.Preconditions;

/**
 * @author summer
 *
 */
public class ElasticSearchSingleClient{

	/**
	 * 
	 */
	
	private static volatile TransportClient client;
	
	private Settings settings;
	
	private ElasticSearchSingleClient() {
		
	}

	public TransportClient create() {
		
		TransportClient ins = client;
		if(ins == null){
			synchronized(ElasticSearchSingleClient.class){
				if(ins == null){
					ins = new TransportClient.Builder()
					.settings(settings)
					.build();
				}
				
				client = ins;
			}
		}
		return client;
	}

	public static ElasticSearchSingleClient builder() {
		
		return new ElasticSearchSingleClient();
	}
	
	public ElasticSearchSingleClient settings(Settings settings){
		
		Preconditions.checkArgument(settings != null,"elasticsearch configurations is null");
		this.settings = settings;
		return this;
	}

}
