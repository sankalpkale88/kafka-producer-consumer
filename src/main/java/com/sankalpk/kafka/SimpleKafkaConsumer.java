/**
 * 
 */
package com.sankalpk.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * @author sankalpk
 *
 */
public class SimpleKafkaConsumer {
  
  private static final String TOPIC_NAME = "test";
  
  public static void main(String[] args) {
    
    Properties props = getKafkaProperties();
    
    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
     
     //Kafka Consumer subscribes list of topics here.
     consumer.subscribe(Arrays.asList(TOPIC_NAME));
     
     //print the topic name
     System.out.println("Subscribed to topic " + TOPIC_NAME);
     int i = 0;
     
     while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        for (ConsumerRecord<String, String> record : records)
        
        // print the offset,key and value for the consumer records.
        System.out.printf("offset = %d, key = %s, value = %s\n", 
           record.offset(), record.key(), record.value());
     }
    
  }
  
  private static Properties getKafkaProperties() {
    // create instance for properties to access producer configs   
    Properties props = new Properties();
    
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "test");
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", StringDeserializer.class.getName());
    props.put("value.deserializer",StringDeserializer.class.getName());

    return props;
  }
}
