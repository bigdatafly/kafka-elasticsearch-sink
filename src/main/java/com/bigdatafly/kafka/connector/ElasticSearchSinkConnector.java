/**
 * 
 */
package com.bigdatafly.kafka.connector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
//import org.apache.zookeeper.Version;

/**
 * @author summer
 *
 */
public class ElasticSearchSinkConnector extends SinkConnector{

	/**
	 * 
	 */
	
	private Map<String, String> conf;
	

	@Override
	public void start(Map<String, String> conf) {
		
		this.conf = conf;
		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Class<? extends Task> taskClass() {
		
		return ElasticSearchSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		
		List<Map<String,String>> taskConfigs = new ArrayList<Map<String,String>>();
		Map<String,String> confs = new HashMap<String,String>();
		for(int i = 0;i<maxTasks;i++){
			confs.putAll(this.conf);
			taskConfigs.add(confs);
		}
		
		return taskConfigs;
	}

	@Override
	public String version() {
		
		return Version.version();
	}

}
