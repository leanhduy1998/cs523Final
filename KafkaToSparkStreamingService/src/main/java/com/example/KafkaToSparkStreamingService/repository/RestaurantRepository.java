package com.example.KafkaToSparkStreamingService.repository;

import com.example.KafkaToSparkStreamingService.entity.Restaurant;
import com.example.KafkaToSparkStreamingService.hbase.HbaseConnection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


public class RestaurantRepository implements Serializable {
        private static final long serialVersionUID = 1L;
        private static RestaurantRepository instance;
        private static final String TABLE_NAME = "restaurant";
        private static final String CF = "cf";
        private static SparkSession spark;
	private RestaurantRepository() {}
        public static RestaurantRepository getInstance() {
            if (instance == null) {
                instance = new RestaurantRepository();
            }
            return instance;
        }
        public void createTable() {
            Connection connection = HbaseConnection.getInstance();
            try (Admin admin = connection.getAdmin()) {
                HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
                table.addFamily(new HColumnDescriptor(CF).setCompressionType(Algorithm.NONE));
                if (!admin.tableExists(table.getTableName())) {
                    System.out.println("Creating table ");
                    admin.createTable(table);
                    System.out.println("Table created");
                }
            } catch (IOException ex) {
                System.out.println(ex.getMessage());
            }
        }
    public void dropTableHbase() throws IOException {
        Connection connection = HbaseConnection.getInstance();
            try(Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf(TABLE_NAME);
            if (admin.tableExists(tableName)) {
                // If the table exists, disable it and then delete it
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println("Table '" + tableName + "' deleted.");
            } else {
                System.out.println("Table '" + tableName + "' does not exist.");
            }
        }
    }

    public void save(Configuration config, JavaRDD<Restaurant> records)
            throws MasterNotRunningException, Exception {
        Job job = Job.getInstance(config);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);
        job.setOutputFormatClass(TableOutputFormat.class);
        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = records.mapToPair(new MyPairFunction());
        hbasePuts.saveAsNewAPIHadoopDataset(job.getConfiguration());
    }

        private Put putObject(String key, Restaurant obj) {
            Put p = new Put(Bytes.toBytes(key));
            p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("_id"), Bytes.toBytes(obj.get_id()));
            p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("name"), Bytes.toBytes(obj.getName()));
            p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("latitude"), Bytes.toBytes(obj.getAddress().getLatitude()));
            p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("longitude"), Bytes.toBytes(obj.getAddress().getLongitude()));
            p.addColumn(Bytes.toBytes(CF), Bytes.toBytes("rating"), Bytes.toBytes(obj.getWeighted_rating_value()));

            return p;
        }

        public Restaurant get(Connection connection, String key) throws IOException {
            try (Table tb = connection.getTable(TableName.valueOf(TABLE_NAME))) {
                Get g = new Get(Bytes.toBytes(key));
                Result result = tb.get(g);
                if (result.isEmpty()) {
                    return null;
                }
                byte[] value1 = result.getValue(Bytes.toBytes(CF), Bytes.toBytes("_id"));
                byte[] value2 = result.getValue(Bytes.toBytes(CF), Bytes.toBytes("name"));
                byte[] value3 = result.getValue(Bytes.toBytes(CF), Bytes.toBytes("latitude"));
                byte[] value4 = result.getValue(Bytes.toBytes(CF), Bytes.toBytes("longitude"));
                byte[] value5 = result.getValue(Bytes.toBytes(CF), Bytes.toBytes("rating"));
                return Restaurant.of(
                        Bytes.toString(value1),
                        Bytes.toString(value2),
                        Bytes.toString(value3),
                        Bytes.toString(value4),
                        Bytes.toString(value5)
                );
            }
        }
//    public void fetchDataFromHbase(Connection connection) throws IOException {
//
//        // Specify the table name
//        TableName tableName = TableName.valueOf(TABLE_NAME);
//
//        // Create a scanner to read all rows from the table
//        Scan scan = new Scan();
//
//        // Create a table instance
//        Table table = connection.getTable(tableName);
//
//        // Retrieve all rows from the table using streaming
//        table.getScanner(scan).forEach(result -> {
//            // Process each row's data here
//            byte[] rowKey = result.getRow();
//            // Example: Print the row key as a string
//            System.out.println("Row Key: " + Bytes.toString(rowKey));
//
//            // Iterate through the columns and values
//            result.listCells().forEach(cell -> {
//                String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength());
//                String qualifier = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
//                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
//
//                // Example: Print family, qualifier, and value
//                System.out.println("Family: " + family);
//                System.out.println("Qualifier: " + qualifier);
//                System.out.println("Value: " + value);
//            });
//
//            System.out.println("-----------------------------------");
//        });
//
//        // Close the HBase connection
//        connection.close();
//    }
//    public void fetchDataFromHbase(Connection connection) throws IOException {
//
//        TableName tableName = TableName.valueOf(TABLE_NAME);
//
//        try (Table table = connection.getTable(tableName)) {
//            Scan scan = new Scan();
//            try (ResultScanner scanner = table.getScanner(scan)) {
//                for (Result result : scanner) {
//                    for (Cell cell : result.rawCells()) {
//                        String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
//                        String columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
//                        String columnQualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
//                        String cellValue = Bytes.toString(CellUtil.cloneValue(cell));
//
//                        System.out.println("Row Key: " + rowKey);
//                        System.out.println("Column Family: " + columnFamily);
//                        System.out.println("Column Qualifier: " + columnQualifier);
//                        System.out.println("Cell Value: " + cellValue);
//                        System.out.println("-------------------");
//                    }
//                }
//            }
//        }
//    }
    public void fetchDataFromHbase(Connection connection) throws IOException {
        TableName tableName = TableName.valueOf(TABLE_NAME);
        Table table = connection.getTable(tableName);

        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result result : resultScanner) {
            String id = Bytes.toString(result.getValue(Bytes.toBytes(CF), Bytes.toBytes("_id")));
            String name = Bytes.toString(result.getValue(Bytes.toBytes(CF), Bytes.toBytes("name")));
            String latitude = Bytes.toString(result.getValue(Bytes.toBytes(CF), Bytes.toBytes("latitude")));
            String longitude = Bytes.toString(result.getValue(Bytes.toBytes(CF), Bytes.toBytes("longitude")));
            String rating = Bytes.toString(result.getValue(Bytes.toBytes(CF), Bytes.toBytes("rating")));

            System.out.println("Data fetching from Hbase table restaurant");
            System.out.println("Id: " + id);
            System.out.println("Name: " + name);
            System.out.println("Latitude: " + latitude);
            System.out.println("Longitude: " + longitude);
            System.out.println("Rating: " + rating);
            System.out.println("----------------------------------");
        }
        resultScanner.close();
    }


    class MyPairFunction implements PairFunction<Restaurant, ImmutableBytesWritable, Put> {

        private static final long serialVersionUID = 1L;
        @Override
        public Tuple2<ImmutableBytesWritable, Put> call(Restaurant record) throws Exception {
            String key = "key";
            Put put = putObject(key, record);
            return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), put);
        }
    }

}

