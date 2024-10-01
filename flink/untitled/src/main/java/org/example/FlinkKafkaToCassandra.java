package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;


public class FlinkKafkaToCassandra {
    public static void main(String[] args) throws Exception {

        // Set up the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("flink-test")
                .setGroupId("flink-consumer")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();



        DataStream<String> kafkaStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

        kafkaStream.addSink(new CassandraSink());

        env.execute("Flink Kafka to Cassandra");
    }

    // Cassandra Sink Function
    public static class CassandraSink implements SinkFunction<String> {
        private transient Cluster cluster;
        private transient Session session;

        @Override
        public void invoke(String value, Context context) {
            // Initialize Cassandra connection
            if (session == null) {
                cluster = Cluster.builder().addContactPoint("cassandra").build();
                session = cluster.connect("test_keyspace");
            }
            // Insert into Cassandra
            session.execute("INSERT INTO test_table (id, value) VALUES (uuid(), '" + value + "')");
        }

//        @Override
//        public void close() {
//            if (session != null) {
//                session.close();
//            }
//            if (cluster != null) {
//                cluster.close();
//            }
//        }
    }
}
