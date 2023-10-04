package com.example.FetchDataFromHbaseService;


import com.example.KafkaToSparkStreamingService.entity.Restaurant;
import com.example.KafkaToSparkStreamingService.hbase.HbaseConnection;
import com.example.KafkaToSparkStreamingService.repository.RestaurantRepository;
import org.apache.hadoop.hbase.client.Connection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;


@Service
public class FetchDataFromHbaseService {
    @Autowired
    public FetchDataFromHbaseService() throws InterruptedException, IOException {
        Connection connection = HbaseConnection.getInstance();
        fetchDataFromHbase(connection);

    }

//    public void fetchDataFromHbase(Connection connection) throws IOException {
//        RestaurantRepository repository = RestaurantRepository.getInstance();
//        repository.fetchDataFromHbase(connection);
//    }
    public void fetchDataFromHbase(Connection connection) throws IOException {
        RestaurantRepository repository = RestaurantRepository.getInstance();
        repository.fetchDataFromHbase(connection);
    }
}


