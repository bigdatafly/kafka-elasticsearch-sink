/**
 * 
 */
package com.bigdatafly.elasticsearch;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import jersey.repackaged.com.google.common.base.Preconditions;

import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.hadoop.util.StringUtils;

/**
 * @author summer
 *
 */
public class ElasticSearchConfiguration {

	/**
	 * 
	 */
	public ElasticSearchConfiguration() {
		// TODO Auto-generated constructor stub
	}

	public TransportAddress[] getNodesTransportAddress() {
		
		List<String> nodes = null;
		return getNodesTransportAddress(nodes);
	}

	private TransportAddress[] getNodesTransportAddress(List<String> nodes){
		
		TransportAddress[] transportAddress = new TransportAddress[nodes.size()];
		int i = 0;
		for(String node : nodes){
			
			transportAddress[i++] = getTransportAddress(node);
		}
		
		return transportAddress;
	}
	
	private TransportAddress getTransportAddress(String sevrName){
		
		Preconditions.checkArgument(StringUtils
				.hasText(sevrName),"server name "+ sevrName+" is null");
		String host = null;
		int port = ElasticSearchConstants.ES_DEFAULT_PORT;
		
		
		
		InetSocketTransportAddress sevrAddr = null;
		try {
			sevrAddr = new InetSocketTransportAddress(InetAddress.getByName(host), port);
		} catch (UnknownHostException e) {
			
			e.printStackTrace();
		}
		
		return sevrAddr;
	}
	
	public Object get(String clusterName) {
		// TODO Auto-generated method stub
		return null;
	}

}
