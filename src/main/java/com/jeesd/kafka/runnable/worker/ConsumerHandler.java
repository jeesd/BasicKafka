package com.jeesd.kafka.runnable.worker;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerHandler {

	//使用一个consumer将消息放入后端队列，你当然可以使用前一种方法中的多实例按照某张规则同时把消息放入后端队列
	private  KafkaConsumer<String, String> consumer;
	private ExecutorService executors;
	
	public ConsumerHandler(String brokerList, String groupId, String topic) {
		Properties props = new Properties();
		props.put("bootstrap.servers", brokerList);
		props.put("group.id", groupId);
		//自动提交位移
		props.put("enable.auto.commit", "true"); 
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		consumer = new KafkaConsumer<String, String>(props);
		// 分区副本自动分配策略
		consumer.subscribe(Arrays.asList(topic));   
	}
	
	public void execute(int workerNum) {
		executors = Executors.newFixedThreadPool(workerNum);
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(200);
		    for (final ConsumerRecord<String, String> record : records) {
	                 executors.submit(new Worker(record));
             }
        }
	 }
		
     public void shutdown() {
         if (consumer != null) {
             consumer.close();
        }
         if (executors != null) {
             executors.shutdown();
         }
         try {
            if (!executors.awaitTermination(10, TimeUnit.SECONDS)) {
                System.out.println("Timeout.... Ignore for this case");
             }
        } catch (InterruptedException ignored) {
             System.out.println("Other thread interrupted this shutdown, ignore for this case.");
             Thread.currentThread().interrupt();
        }
    }
}
