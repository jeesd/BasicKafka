package com.jeesd.spring.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;

public class KafkaConsumerServer implements MessageListener<String, String> {

	public void onMessage(ConsumerRecord<String, String> record) {
		
		System.out.println("kafkaConsumer开始消费=============");
         String topic = record.topic();
         String key = record.key();
         String value = record.value();
         long offset = record.offset();
         int partition = record.partition();
         System.out.println("-------------topic:"+topic);
         System.out.println("-------------value:"+value);
         System.out.println("-------------key:"+key);
         System.out.println("-------------offset:"+offset);
         System.out.println("-------------partition:"+partition);
         System.out.println("kafkaConsumer结束消费=============");
	}

}
