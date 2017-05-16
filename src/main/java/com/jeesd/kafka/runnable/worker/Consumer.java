package com.jeesd.kafka.runnable.worker;

//可独立扩展consumer数和worker数，伸缩性好
//通常难于维护分区内的消息顺序
//处理链路变长，导致难以保证提交位移的语义正确性
public class Consumer {

	public static void main(String[] args) {
		
		String brokerList = "localhost:9092";
		String groupId = "testGroup1";
		String topic = "test-topic";
		int workerNum = 3;
		
		ConsumerHandler consumers = new ConsumerHandler(brokerList, groupId, topic);
		try {
			consumers.execute(workerNum);
			Thread.sleep(100000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			consumers.shutdown();
		}
	}
}
