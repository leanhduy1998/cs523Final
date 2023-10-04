package com.example.KafkaToSparkStreamingService.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;


public class HbaseConnection {

    private static Connection connection;

    public static Connection getInstance() {
        if (connection == null) {
            try {
                Configuration config = HBaseConfiguration.create();
                connection = ConnectionFactory.createConnection(config);
            } catch (IOException ex) {
                System.out.println(ex.getMessage());
                System.exit(0);
            }
        }
        return connection;
    }
}
