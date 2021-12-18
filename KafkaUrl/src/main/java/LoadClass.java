import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

public class LoadClass extends Thread {
    public String getData() throws  Exception{
        URL url = new URL("https://randomuser.me/api/0.8/?results=5");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        //conn.connect();
        conn.setRequestMethod("GET");
        int response_code = conn.getResponseCode();
        System.out.println("The response code is "+response_code);
        BufferedReader in = new BufferedReader(new InputStreamReader(
                conn.getInputStream()));
        StringBuffer response =  new StringBuffer();
        String inputLine;
        while ((inputLine = in.readLine()) != null) {
            response.append(inputLine);
        }
        in.close();

        // print result
        System.out.println(response.toString());
        return response.toString();
    }
    public void call_producer(String record)
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
        String finalResult = record.toString();
        System.out.println("The final result is " + finalResult);
        ProducerRecord<String, String> producer = new ProducerRecord<>("kanisgar", Integer.toString(1), finalResult);
        kafkaProducer.send(producer);
        kafkaProducer.close();
        System.out.println("Produced the record");
        //System.out.println("Calling the consumer");
        //call_consumer();
    }
    public void run() {
        while (true) {
            try {
                String data = getData();
                call_producer(data);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}
