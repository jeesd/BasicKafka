package com.jeesd.kafka.runnable;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerRunnable implements Runnable {

	// 每个线程维护私有的KafkaConsumer实例
	private  KafkaConsumer<String, String> consumer;
	
	public ConsumerRunnable(String brokerList, String groupId, String topic) {
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
	
	public void run() {
		while (true) {
		 //200ms作为获取超时时间
		 ConsumerRecords<String, String> records = consumer.poll(200); 
			for (ConsumerRecord<String, String> record : records) {
			// 这里面写处理消息的逻辑，这里只是简单地打印消息
			System.out.println(Thread.currentThread().getName() + " consumed " + record.partition() +
			"th message with offset: " + record.offset());
			}
		}
	}

}
