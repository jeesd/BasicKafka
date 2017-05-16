package com.jeesd.kafka.runnable;

//方便实现
//速度较快，因为不需要任何线程间交互
//易于维护分区内的消息顺序
//更多的TCP连接开销(每个线程都要维护若干个TCP连接)
//consumer数受限于topic分区数，扩展性差
//频繁请求导致吞吐量下降
//线程自己处理消费到的消息可能会导致超时，从而造成rebalance
public class Consumer {

	public static void main(String[] args) {
		String brokerList = "localhost:9092";
		String groupId = "testGroup1";
		String topic = "test-topic";
		int consumerNum = 3;
		
		ConsumerGroup consumerGroup = new ConsumerGroup(consumerNum, groupId, topic, brokerList);
		consumerGroup.execute();
	}
}
