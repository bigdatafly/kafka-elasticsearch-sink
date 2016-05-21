package com.bigdatafly.elasticsearch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkProcessor.Listener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdatafly.factory.SingletonFactory;

public class ElasticSearchSingletonBulkProcessor{

	private static final Logger logger = LoggerFactory.getLogger(ElasticSearchSingletonBulkProcessor.class);
	
	private ElasticSearchSingletonClient client;
	private List<ActionRequest> failedRequests = Collections.synchronizedList(new ArrayList<>());
	
    private AtomicInteger counter = new AtomicInteger();
    private Semaphore semaphore = new Semaphore(1);
    
	public ElasticSearchSingletonBulkProcessor(ElasticSearchSingletonClient client) {
		
		this.client = client;
	}

	public BulkProcessor getBulkProcessor(){
		
		return builder.get();
	}
	
	public void waitUntilNonePending() {
		
		while (true) {
            /* ensure no requests are pending */
            semaphore.acquireUninterruptibly();
            if (counter.get() > 0) {
                BulkProcessor bulkProcessor = getBulkProcessor();
                List<ActionRequest> requests = null;
                try {
                    requests = new ArrayList<ActionRequest>(failedRequests);
                    failedRequests.clear();
                } finally {
                    semaphore.release();
                }
                if (!(requests == null || requests.isEmpty())) {
                    for (ActionRequest request : requests) {
                        bulkProcessor.add(request);
                    }
                }
                bulkProcessor.flush();
            } else {
                semaphore.release();
                break;
            }
        }
		
	}
	
	SingletonFactory.Singleton<BulkProcessor> builder = SingletonFactory.of(new Supplier<BulkProcessor>(){
			@Override
			public BulkProcessor get() {
				
				return BulkProcessor.builder(client.getClient(),new Listener(){

					@Override
					public void beforeBulk(long executionId, BulkRequest request) {
						
						semaphore.acquireUninterruptibly();
						
					}

					private List<ActionRequest> requestsOf(BulkRequest request) {
		                return request.requests();
		            }
					
					@Override
					public void afterBulk(long executionId,
							BulkRequest request, BulkResponse response) {
						
						 if (response != null) {
			                    List<ActionRequest> requests = requestsOf(request);
			                    BulkItemResponse[] items = response.getItems();

			                    int done = 0;
			                    for (int i = 0; i < items.length; i++) {
			                        BulkItemResponse item = items[i];
			                        if (item.isFailed()) {
			                            failedRequests.add(requests.get(i));
			                        } else {
			                            done++;
			                        }
			                    }

			                    if (done > 0) {
			                        updatePendingRequests(-done);
			                    }
			                }
			                semaphore.release();
						
					}

					@Override
					public void afterBulk(long executionId,
							BulkRequest request, Throwable failure) {
						
						 if (failure != null) {
			                    if (logger.isErrorEnabled()) {
			                        logger.error("Failed to execute BulkRequest", failure);
			                    }
			                }
			                failedRequests.addAll(requestsOf(request));
			                semaphore.release();
						
					}}).build();
			}
			
		});
	
	public void updatePendingRequests(int count) {
		
		boolean success = false;
        while (!success) {
            int expect = counter.get();
            success = counter.compareAndSet(expect, expect + count);
        }
		
	}
	
	public void close() {
		
		getBulkProcessor().close();
		
	}

	public void add(String index, String opType, byte[] record) {
		
        if (record != null) {
        	getBulkProcessor().add(Requests.indexRequest(index).type(opType).source(record));
            
        }
		
	}
	
}
