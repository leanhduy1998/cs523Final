package com.example.KafkaProducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import  org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProducerC {
    static String topic = "cs523";
    static final String KAFKA_SERVER_URL = "localhost";
    static final int KAFKA_SERVER_PORT = 9092;

    static KafkaProducer<String, String> producer;

    public KafkaProducerC() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(properties);
    }

    public static void produce(String key, String value) throws ExecutionException, InterruptedException {
        long startTime = System.currentTimeMillis();
        RecordMetadata metadata = producer.send(new ProducerRecord<>(
                topic,
                key,
                value)).get();
        System.out.println(
                "Producer Message with id: '" + key + "' sent to partition(" + metadata.partition() + "), offset(" + metadata.offset() + ") in " + (System.currentTimeMillis() - startTime) + " ms");
    }

    public static String fetchRestaurants(double d, double f, String query) {
        URL url;
        try {
            String urlString = "https://api.spoonacular.com/food/restaurants/search?query=" + query + "&lat=" + d + "&lng="+ f + "&apiKey=5c7acd3c1bac40d2b176528ecd7b277f&page=0";

            url = new URL(urlString);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");

            con.setRequestProperty("Content-Type", "application/json");
            con.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11");

            con.setConnectTimeout(5000);
            con.setReadTimeout(5000);

            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer content = new StringBuffer();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            in.close();

            con.disconnect();

            return content.toString();

        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }
}
