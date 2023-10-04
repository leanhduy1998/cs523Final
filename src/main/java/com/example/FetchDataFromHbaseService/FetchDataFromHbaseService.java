package com.example.FetchDataFromHbaseService;


import com.example.KafkaToSparkStreamingService.config.KafkaConstant;
import com.example.KafkaToSparkStreamingService.entity.Restaurant;
import com.example.KafkaToSparkStreamingService.hbase.HbaseConnection;
import com.example.KafkaToSparkStreamingService.repository.RestaurantRepository;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
//import org.apache.hadoop.hbase.spark.JavaHBaseContext;


@Service
public class FetchDataFromHbaseService {
    @Autowired
    public FetchDataFromHbaseService() throws InterruptedException, IOException {
        Connection connection = HbaseConnection.getInstance();
        String key = "key";
        //fetchDataFromHbase(connection,key);
        fetchAllDataFromHbase(connection,key);
    }

//    public void fetchDataFromHbase(Connection connection, String key) throws IOException {
//        RestaurantRepository repository = RestaurantRepository.getInstance();
//        Restaurant restaurant =  repository.get(connection,key);
//        System.out.println("Name from Hbase:" + restaurant.getName());
//        System.out.println("Latitude from Hbase:" +restaurant.getAddress().getLatitude());
//        System.out.println("Longitude from Hbase:" +restaurant.getAddress().getLongitude());
//        System.out.println("Rating from Hbase:"+restaurant.getWeighted_rating_value());
//    }
    public void fetchAllDataFromHbase(Connection connection, String key) throws IOException {
        RestaurantRepository repository = RestaurantRepository.getInstance();
        repository.fetchAllDataFromHbase(connection,key);
    }
}


