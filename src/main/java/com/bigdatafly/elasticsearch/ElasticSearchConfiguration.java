/**
 * 
 */
package com.bigdatafly.elasticsearch;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author summer
 *
 */
public class ElasticSearchConfiguration extends AbstractConfig {

	private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConfiguration.class);
	
	static ConfigDef configDef = new ConfigDef()
		.define(ElasticSearchConstants.CLUSTER_NAME_KEY, Type.STRING,Importance.HIGH, "Elastic search cluster");
	
	/**
	 * 
	 */
	public ElasticSearchConfiguration(Map<String,String> settings) {
		
		super(configDef , settings);
	}

	public TransportAddress[] getNodesTransportAddress() {
		
		List<String> nodes = null;
		String svrName = ;
		
		return getNodesTransportAddress(nodes);
	}

	private TransportAddress[] getNodesTransportAddress(List<String> nodes){
		
		List<TransportAddress> transportAddress = new ArrayList<TransportAddress>(nodes.size());
		for(String node : nodes){
			TransportAddress transAddr = getTransportAddress(node);
			if(transAddr == null)
				continue;
			transportAddress.add(transAddr);
		}
		
		return transportAddress.toArray(new TransportAddress[transportAddress.size()]);
	}
	
	private TransportAddress getTransportAddress(String sevrName){
		
		Preconditions.checkArgument(StringUtils
				.hasText(sevrName),"server name "+ sevrName+" is null");
		String host = null;
		int port = ElasticSearchConstants.DEFAULT_PORT;
		InetSocketTransportAddress sevrAddr = null;	
		try {
			String[] hostAndPort = sevrName.split(":");
			host = hostAndPort[0];
			if(hostAndPort.length == 2){
				port = Integer.valueOf(hostAndPort[1]);
			}
			sevrAddr = new InetSocketTransportAddress(InetAddress.getByName(host), port);
		} catch (UnknownHostException | NumberFormatException e) {
			
			logger.error("elasticsearch servername "+ sevrName +" is wrong", e);
		} 
		
		return sevrAddr;
	}
	
	public String getString(String key) {
		
		return super.getString(key);
	}
	
	public String getString(String key,String defVal){
		
		try{
			return super.getString(key);
		}catch(ConfigException e){
			return defVal;
		}
	}

}
