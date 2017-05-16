package com.jeesd.kafka.runnable.worker;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class Worker implements Runnable{
	
	private ConsumerRecord<String, String> consumerRecord;
	
	public Worker(ConsumerRecord<String, String> record) {
		this.consumerRecord = record;
	}

	public void run() {
	  // 这里写你的消息处理逻辑，本例中只是简单地打印消息
	  System.out.println(Thread.currentThread().getName() + " consumed " + consumerRecord.partition()
		 + "th message with offset: " + consumerRecord.offset());

	}

}
