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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.springframework.beans.factory.annotation.Autowired;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class RestaurantRepository implements Serializable {
        private static final long serialVersionUID = 1L;
        private static RestaurantRepository instance;
        private static final String TABLE_NAME = "restaurant";
        private static final String CF = "cf";
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

    public void save(Configuration config, JavaRDD<Restaurant> records)
            throws MasterNotRunningException, Exception {
        Job job = Job.getInstance(config);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);
        job.setOutputFormatClass(TableOutputFormat.class);
        JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = records.mapToPair(new MyPairFunction());
        hbasePuts.saveAsNewAPIHadoopDataset(job.getConfiguration());
    }


    public void putAll(Map<String, Restaurant> data) throws IOException {
            Connection connection = HbaseConnection.getInstance();
            try (Table tb = connection.getTable(TableName.valueOf("restaurant"))) {
                ArrayList<Put> ls = new ArrayList<Put>();
                for (String k : data.keySet()) {
                    ls.add(putObject(k, data.get(k)));
                }
                tb.put(ls);
            }
        }

        public void put(String key, Restaurant obj) throws IOException {
            Connection connection = HbaseConnection.getInstance();
            try (Table tb = connection.getTable(TableName.valueOf(TABLE_NAME))) {
                tb.put(putObject(key, obj));
            }
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
    public void fetchAllDataFromHbase(Connection connection, String key) throws IOException {
        TableName tableName = TableName.valueOf(TABLE_NAME);
        Table table = connection.getTable(tableName);

        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);

        for (Result result : resultScanner) {
            // Iterate through the rows
            for (Cell cell : result.rawCells()) {
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                String columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
                String columnQualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String cellValue = Bytes.toString(CellUtil.cloneValue(cell));

                System.out.println("Row Key: " + rowKey);
                System.out.println("Column Family: " + columnFamily);
                System.out.println("Column Qualifier: " + columnQualifier);
                System.out.println("Cell Value: " + cellValue);
                System.out.println("-------------------");
            }
        }
        resultScanner.close();
    }
    public void dropTableHbase() throws IOException, InterruptedException {

        try(Connection connection = HbaseConnection.getInstance();
        Admin admin = connection.getAdmin()) {
                TableName tableName = TableName.valueOf(TABLE_NAME);
                if (admin.tableExists(tableName)) {
                    // If the table exists, disable it and then delete it
                    admin.disableTable(tableName);
                    admin.deleteTable(tableName);
                    System.out.println("Table '" + tableName + "' deleted.");
                } else {
                    System.out.println("Table '" + tableName + "' does not exist.");
                }
                connection.close();
            }
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

