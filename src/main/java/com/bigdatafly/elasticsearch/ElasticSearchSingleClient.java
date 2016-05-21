/**
 * 
 */
package com.bigdatafly.elasticsearch;

import java.util.function.Supplier;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;

import com.bigdatafly.factory.SingletonFactory;
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

	/**
	 * lambda 就是NB
	 * @return
	 */
	public TransportClient create() {
		
		Preconditions.checkArgument(settings != null,
				"elasticsearch configurations is null");
		
		return  SingletonFactory.of(new Supplier<TransportClient>(){
			@Override
			public TransportClient get() {
				return new TransportClient.Builder()
							.settings(settings)
							.build();
			}
		}).get();
	}

	public static ElasticSearchSingleClient builder() {
		
		return new ElasticSearchSingleClient();
	}
	
	public ElasticSearchSingleClient settings(Settings settings){
		
		this.settings = settings;
		return this;
	}

}
