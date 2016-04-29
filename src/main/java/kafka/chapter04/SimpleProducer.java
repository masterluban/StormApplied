package kafka.chapter04;


import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class SimpleProducer {
	
	private static Producer<String,String> producer;
	
	public SimpleProducer(){
		Properties props = new Properties();
		//props.put("metadata.broker.list", "192.168.174.130:9092");
		props.put("metadata.broker.list", "stormmaster:9092");
		
		//props.put("metadata.broker.list", "localhost:9092");
	
		props.put("serializer.class","kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String,String>(config);
	}
	
	public static void main(String[] args){
		String topic = "kafkatopic";
		String count = "10";
		int messageCount = Integer.parseInt(count);
		SimpleProducer simpleProducer = new SimpleProducer();
		simpleProducer.publishMessage(topic, messageCount);
	}
	
	private void publishMessage(String topic, int messageCount){
		for (int mCount =0; mCount < messageCount; mCount++){
			String runtime = new Date().toString();
			String msg = "Message publishing time - " + runtime;
			System.out.println(msg);
			KeyedMessage<String,String> data = new KeyedMessage<String, String>(topic,msg);
			producer.send(data);
		}
		producer.close();
	}

}
