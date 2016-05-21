/**
 * 
 */
package com.bigdatafly.kafka.connector;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import com.bigdatafly.elasticsearch.ElasticSearchClient;
import com.bigdatafly.elasticsearch.ElasticSearchConfiguration;

/**
 * @author summer
 *
 */
public class ElasticSearchSinkTask extends SinkTask{

	
	private ElasticSearchClient client;
	private ElasticSearchConfiguration configuration;
	
	public ElasticSearchSinkTask(){
		super();
	}
	
	@Override
	public void start(Map<String, String> props) {
		
		this.configuration = new ElasticSearchConfiguration(props);
		this.client = new ElasticSearchClient(configuration);
		this.client.start();
	}

	@Override
	public void put(Collection<SinkRecord> records) {
		
		if (records != null) {
            int count = 0;
            for (SinkRecord record : records) {
            	byte[] data = serialize(record);
                boolean processed = client.process(data);
                if (processed) {
                    count++;
                }
            }
            client.updatePendingRequests(count);
        }
	}

   
	protected byte[] serialize(SinkRecord record){
		
        return Serialize.builder().serialize(record);
	}
	
	
	
	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
		
		client.waitUntilNonePending();
	}

	@Override
	public void stop() {
		
		client.shutdown();
		
	}
	
	public String version() {
		
		return Version.version();
	}

	public static class Serialize{
		
		public JsonConverter serialize = new JsonConverter();

		public Serialize(){
			
			 Map<String, String> props = new HashMap<>();
		     props.put("schemas.enable", Boolean.FALSE.toString());
		     serialize.configure(props, false);
		}
       
		
		protected byte[] serialize(SinkRecord record){
			
			return serialize.fromConnectData(record.topic(), record.valueSchema(), record.value());
		}
		
		public static Serialize builder(){
			
			return new Serialize();
		}
	}

}
