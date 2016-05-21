/**
 * 
 */
package com.bigdatafly.kafka.connector;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

/**
 * @author summer
 *
 */
public class ElasticSearchSinkTask extends SinkTask{

	/**
	 * 
	 */
	public ElasticSearchSinkTask() {
		// TODO Auto-generated constructor stub
	}

	public String version() {
		
		return Version.version();
	}

	@Override
	public void start(Map<String, String> props) {
		
		
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stop() {
		
		
	}

}
