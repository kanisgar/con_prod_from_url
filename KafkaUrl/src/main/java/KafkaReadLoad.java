import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class KafkaReadLoad extends Thread {
    public void call_producer(JsonNode record)
    {
        Properties properties = new Properties();
        String bootStrapServer = "127.0.0.1:9092";
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, IKafkaConstants.CLIENT_ID);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    /*properties.put("bootstrap.servers", "localhost:9092");
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");*/
        KafkaProducer kafkaProducer = new KafkaProducer<>(properties);
        // String id = record.get("id").asText().trim();
        //String name= record.get("name").asText().trim();
        StringBuffer sb = new StringBuffer();
        //String finalResult = sb.append(id).append("|").append(name).toString();
        String finalResult=record.get("results").toString();
        System.out.println("The final result is " + finalResult);
        ProducerRecord<String, String> producer = new ProducerRecord<>("prabu", Integer.toString(1), finalResult);
        kafkaProducer.send(producer);
        kafkaProducer.close();
        System.out.println("Produced the record");
        //System.out.println("Calling the consumer");
        //call_consumer();
    }
    public void call_consumer()
    {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("group.id", "test-group");

        KafkaConsumer kafkaConsumer = new KafkaConsumer(properties);
        List topics = new ArrayList();
        topics.add("kanisgar");
        kafkaConsumer.subscribe(topics);
        try{
            while (true){
                ConsumerRecords<String,String> records = kafkaConsumer.poll(10);
                for (ConsumerRecord<String,String> record: records){
                    System.out.println(String.format("Topic - %s, Partition - %d, Value: %s, Offest is : %d", record.topic(), record.partition(), record.value(),record.offset()));
                    ObjectMapper objectMapper = new ObjectMapper();
                    JsonNode jso = objectMapper.readValue(record.value(),JsonNode.class);
                    System.out.println("The Record is " + jso );
                    call_producer(jso);
                }
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            kafkaConsumer.close();
        }
    }
    public  void run()
    {
        call_consumer();
    }
}
