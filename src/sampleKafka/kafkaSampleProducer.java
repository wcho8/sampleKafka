package sampleKafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;

public class kafkaSampleProducer implements Runnable{

	private String name;

	public kafkaSampleProducer(String name) {
		this.name = name;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		String topic = "test";

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		
		props.put("acks", "all");

		props.put("retries", 0);

		props.put("batch.size", 16384);

		props.put("buffer.memory", 33554432);

		props.put("key.serializer", 
				"org.apache.kafka.common.serialization.StringSerializer");

		props.put("value.serializer", 
				"org.apache.kafka.common.serialization.StringSerializer");
		
		Producer<String, String> prod = new KafkaProducer<String, String>(props);
		for(int i = 0; i < 10; i++) {
			String message = this.name + " sends " + Integer.toString(i);
			prod.send(new ProducerRecord<String, String>(topic, message));
			System.out.println(this.name + " sends successfully!");
		}
		
		prod.close();
	}

	public static void main(String[] args) {
		List<kafkaSampleProducer> producers = new ArrayList<>();
		for(int i = 0; i < 3; i++) {
			String name = "thread_" + i;
			kafkaSampleProducer producer = new kafkaSampleProducer(name);
			producers.add(producer);
		}
		
		for(kafkaSampleProducer prod : producers) {
			prod.run();
		}
	}
}
