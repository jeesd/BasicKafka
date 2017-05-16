package com.jeesd.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * kafka消费端
 * @author song
 *
 */
//单线程消费
public class Consumer {

	private static KafkaConsumer<String, String> consumer;

	public static void main(String[] args) {
		
		/*Properties props = new Properties();  
		props.put("bootstrap.servers", "localhost:9092");  
		props.put("group.id", "1");  
		props.put("enable.auto.commit", "true");  
		props.put("auto.commit.interval.ms", "1000");  
		props.put("session.timeout.ms", "30000");  
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  
		consumer = new KafkaConsumer<String, String>(props);  
		consumer.subscribe(Arrays.asList("test"));  
        while(true) {  
            ConsumerRecords<String, String> records = consumer.poll(1000);  
            for(ConsumerRecord<String, String> record : records) {  
                System.out.println("fetched from partition " + record.partition() + ", offset: " + record.offset() + ", message: " + record.value());  
            }  
        }  */
		
		Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("topic1"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records)
                System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
        }
	}
}
