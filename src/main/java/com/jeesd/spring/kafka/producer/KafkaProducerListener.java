package com.jeesd.spring.kafka.producer;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.kafka.support.ProducerListener;

public class KafkaProducerListener implements ProducerListener<Object, Object> {

	public boolean isInterestedInSuccess() {
		// TODO Auto-generated method stub
		System.out.println("监听器启动");
		return true;
	}

	public void onError(String topic, Integer partition, Object key,
            Object value, Exception exception) {
		// TODO Auto-generated method stub
		System.out.println("kafka发送错误数据===========");
		System.out.println("----------topic:"+topic);
		System.out.println("----------partition:"+partition);
		System.out.println("----------key:"+key);
		System.out.println("----------value:"+value);
		System.out.println("----------Exception:"+exception);
	}

	public void onSuccess(String topic, Integer partition, Object key,
            Object value, RecordMetadata recordMetadata) {
		// TODO Auto-generated method stub
		System.out.println("kafka发送成功数据===========");
		System.out.println("----------topic:"+topic);
		System.out.println("----------partition:"+partition);
		System.out.println("----------key:"+key);
		System.out.println("----------value:"+value);
		System.out.println("----------RecordMetadata:"+recordMetadata);
		
	}

}
 