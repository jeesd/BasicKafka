package com.jeesd.kafka;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
/**
 * kafka生产端
 * @author song
 *
 */
public class Producer {
	
	//private static KafkaProducer<String, String> producer;

	public static void main(String[] args) throws InterruptedException {
		
		/*Properties props = new Properties();  
		props.put("bootstrap.servers", "localhost:9092");  
		props.put("acks", "1");  
		props.put("retries", 0);  
		props.put("batch.size", 16384);  
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");  
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");  
		
		producer = new KafkaProducer<String, String>(props);  
		int i = 0;  
        while(true) {  
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("test", String.valueOf(i), "this is message"+i);  
            producer.send(record, new Callback() {  
                public void onCompletion(RecordMetadata metadata, Exception e) {  
                    if (e != null)  
                        e.printStackTrace();  
                    System.out.println("message send to partition " + metadata.partition() + ", offset: " + metadata.offset());  
                }  
            });  
            i++;  
            Thread.sleep(1000);  
        }  */
		
		Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for(int i = 0; i < 100; i++)
            producer.send(new ProducerRecord<String, String>("topic1", Integer.toString(i), Integer.toString(i)));

        producer.close();
	}
	
}
