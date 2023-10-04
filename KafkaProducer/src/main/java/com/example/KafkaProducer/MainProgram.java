package com.example.KafkaProducer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;

@Service
public class MainProgram {
    public static final String TABLE_NAME = "test";
    public static final String CF_ROW = "rowkey";
    public static final String CF_DATA = "data";

        //KafkaConsumerClient consumer = new KafkaConsumerClient();
@Autowired
    public MainProgram() throws ExecutionException, InterruptedException {
    double lat = 37.7786357;
    double lng = -122.3918135;
    KafkaProducerC producer = new KafkaProducerC();

    	String result = producer.fetchRestaurants(lat, lng, "pizza");
    	System.out.println(result);
    String time = String.valueOf(java.time.Instant.now().getEpochSecond());
    producer.produce(time, result);

    //producer.produce(time, result);

    //producer.produce(time, result);
}

}
