package kafka.chapter04;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class CustomPartitionProducer {
	
	private static Producer<String,String> producer;

	public CustomPartitionProducer(){
		Properties props = new Properties();
		props.put("metadata.broker.list", "192.168.174.130:9092, 192.168.174.130:9093, 192.168.174.130:9094");
		props.put("serializer.class","kafka.serializer.StringEncoder");
		props.put("partitioner.class","kafka.chapter04.SimplePartitioner");
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String,String>(config);
	}
	
	public static void main(String[] args){
		String topic = "website-hits2";
		String count = "100";
		int messageCount = Integer.parseInt(count);
		CustomPartitionProducer simpleProducer = new CustomPartitionProducer();
		simpleProducer.publishMessage(topic, messageCount);
	}
	
	private void publishMessage(String topic, int messageCount){
		Random random = new Random();
		for (int mCount =0; mCount < messageCount; mCount++){
			String clientIP = "192.168.14." + random.nextInt(255);
			String accessTime = new Date().toString();
			String message = accessTime + ", kafka.apache.org," + clientIP;
			System.out.println(message);
			KeyedMessage<String,String> data = new KeyedMessage<String, String>(topic,clientIP, message);
			producer.send(data);
		}
		producer.close();
	}
}
