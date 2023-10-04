package com.example.KafkaToSparkStreamingService;


import java.io.IOException;
import java.util.*;


import com.example.KafkaToSparkStreamingService.config.KafkaConstant;
import com.example.KafkaToSparkStreamingService.entity.Restaurant;
import com.example.KafkaToSparkStreamingService.hbase.HbaseConnection;
import com.example.KafkaToSparkStreamingService.repository.RestaurantRepository;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.spark.SparkConf;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;


//import org.apache.spark.api.java.JavaFutureAction;
//import org.apache.spark.api.java.JavaRDD;// new import
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;

import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.google.gson.Gson;
//import org.apache.hadoop.hbase.spark.JavaHBaseContext;


@Service
public class KafkaToSparkStreamingService {
    private static final Gson gson = new Gson();

    @Autowired
    public KafkaToSparkStreamingService() throws InterruptedException, IOException {

        SparkConf sparkConf = new SparkConf().setAppName("SparkApplication").setMaster("local[*]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        //dropTableHbase();
        streamingFromKafka(jsc);
    }

    public static Map<String, Object> getKafkaParams() {
        Map<String, Object> kafkaParams = new HashMap();
        kafkaParams.put("bootstrap.servers", KafkaConstant.KAFKA_BROKERS);
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "id1");
        kafkaParams.put("fetch.message.max.bytes", String.valueOf(KafkaConstant.MESSAGE_SIZE));
        return kafkaParams;
    }

    public static void streamingFromKafka(JavaSparkContext sc) throws IOException, InterruptedException {
        RestaurantRepository repo = RestaurantRepository.getInstance();
        repo.createTable();
        Configuration hadoopConf = sc.hadoopConfiguration();
        try (JavaStreamingContext jssc = new JavaStreamingContext(sc, new Duration(1000))) {
            Set<String> topics = Collections.singleton("cs523");

            JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = KafkaUtils.createDirectStream(
                    jssc,
                    LocationStrategies.PreferConsistent(),
                    ConsumerStrategies.<String, String>Subscribe(topics, getKafkaParams())
            );
            JavaDStream<String> javaDstream = kafkaStream.map(ConsumerRecord::value);
            JavaDStream<List<Restaurant>> restaurantStream = javaDstream.map(jsonString -> Restaurant.parse(jsonString));
            JavaDStream<Restaurant> flatRestaurantStream = restaurantStream.flatMap(
                    restaurantList -> restaurantList.iterator()
            );
            flatRestaurantStream.foreachRDD(restaurantJavaRDD -> {
                repo.save(hadoopConf, restaurantJavaRDD);
               restaurantJavaRDD.foreach(restaurant -> {
                        System.out.println("---------Data written to Hbase--------------");
                        System.out.println("Id: " + restaurant.get_id());
                        System.out.println("Name: " + restaurant.getName());
                        System.out.println("Latitude: " + restaurant.getAddress().getLatitude());
                        System.out.println("Longitude: " + restaurant.getAddress().getLongitude());
                        System.out.println("Rating: "+ restaurant.getWeighted_rating_value());
                        System.out.println("----------------------------------");
               });
            });

            jssc.start();
            jssc.awaitTermination();
        }
    }
    public static void dropTableHbase() throws IOException, InterruptedException {
        RestaurantRepository repo = RestaurantRepository.getInstance();
        repo.dropTableHbase();

    }


}


