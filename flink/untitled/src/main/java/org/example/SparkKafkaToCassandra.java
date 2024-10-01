package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class SparkKafkaToCassandra {
    public static void main(String[] args) throws InterruptedException {

        // Configure Spark
        SparkConf conf = new SparkConf().setAppName("Spark Kafka to Cassandra").setMaster("spark://spark-master:7077")
                .set("spark.cassandra.connection.host", "cassandra");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        // Set Kafka properties
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, "spark-consumer");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Define Kafka topic
        Collection<String> topics = Arrays.asList("flink-test");

        // Create Kafka DStream
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams)
        );

        // Process the stream and write to Cassandra
        stream.map(record -> record.value())
                .foreachRDD(rdd -> {
                    CassandraJavaUtil.javaFunctions(rdd)
                            .writerBuilder("test_keyspace", "test_table", CassandraJavaUtil.mapToRow(String.class))
                            .saveToCassandra();
                });

        jssc.start();
        jssc.awaitTermination();
    }
}
