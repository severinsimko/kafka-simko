package cz.muni.fi.kafka;

import java.io.*;
import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ProducerMain {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
      //  props.put("partitioner.class", "cz.muni.fi.kafka.SimplePartitioner");
        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);

        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(new File("/media/ideapad/Windows7_OS/Java/out")));
            try {
                String line = bufferedReader.readLine();
                while(line!= null){
                    String string = line;
                    KeyedMessage<String,String> data= new KeyedMessage<String, String>("securitycloud-testing-data",string);
                    producer.send(data);
                    line=bufferedReader.readLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }catch(FileNotFoundException e){
            e.printStackTrace();

        }
        producer.close();

    }
}