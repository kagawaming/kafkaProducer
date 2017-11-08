package com.ming.kafkaproducer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


import java.nio.channels.ShutdownChannelGroupException;
import java.nio.charset.Charset;
import java.util.Properties;
import java.util.Random;

import static com.sun.corba.se.spi.activation.IIOP_CLEAR_TEXT.value;

/**
 * Created by mqiu on 8/31/17.
 */
public class producerservice {

    public static void main(String[] args) throws InterruptedException {
        String test = "test1";
        int key = 1;


        Properties properties = new Properties();
        properties.put("bootstrap.servers",  "localhost:9092");
        properties.put("client.id", "DemoProducer");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<Integer, String> _producer = new KafkaProducer<>(properties);
        while(true) {
            byte[] array = new byte[7]; // length is bounded by 7
            new Random().nextBytes(array);
            //String value = new String(array);//, Charset.forName("UTF-8"));
            String value = "abcd";
            System.out.println(value);
            Thread.sleep(5000);
            final int finalKey = key;
            _producer.send(new ProducerRecord<>(test, key, value), new Callback(){
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (metadata != null) {
                        System.out.println("Yeah: " + "topic: topicA, " + "key: " + finalKey + ", value: " + value);
                    } else {
                        System.out.println("Failed to send " + "message");
                    }
                }
            });
            key++;
        }

    }
}
