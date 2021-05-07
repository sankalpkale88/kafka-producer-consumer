/**
 * 
 */
package com.sankalpk.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author sankalpk
 *
 */
public class SimpleKafkaProducer {

  private static final String TOPIC_NAME = "test";
  public static void main(String[] args) {
    
    Properties props = getKafkaProperties();
   
    Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(props);
          
    for(int i = 0; i < 10; i++) {
       producer.send(new ProducerRecord<String, String>(TOPIC_NAME, 
          Integer.toString(i), Integer.toString(i)));
       System.out.println("Message sent successfully");
    }
    producer.close();
    
  }
  
  private static Properties getKafkaProperties() {
    // create instance for properties to access producer configs   
    Properties props = new Properties();
    
    //Assign localhost id
    props.put("bootstrap.servers", "localhost:9092");
    
    //Set acknowledgements for producer requests.      
    props.put("acks", "all");
    
    //If the request fails, the producer can automatically retry,
    props.put("retries", 0);
    
    //Specify buffer size in config
    props.put("batch.size", 16384);
    
    //Reduce the no of requests less than 0   
    props.put("linger.ms", 1);
    
    //The buffer.memory controls the total amount of memory available to the producer for buffering.   
    props.put("buffer.memory", 33554432);
    
    props.put("key.serializer", StringSerializer.class.getName());
       
    props.put("value.serializer",StringSerializer.class.getName());

    return props;
  }
}   
