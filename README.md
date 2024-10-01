# Setup

## Dependencies
* Maven
* Java 11
* Docker
* Colima

## Start Docker agent and run containers
```bash
colimna start
docker-compose up -d
```

## Create a topic and test consumer
```bash
KAFKA_CONTAINER_ID=$(docker ps --filter "name=kafka" --format "{{.ID}}")
docker exec -it "$KAFKA_CONTAINER_ID" /bin/bash
kafka-topics --create --topic flink-test --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-console-producer --topic flink-test --bootstrap-server localhost:9092
kafka-console-consumer --topic flink-test --from-beginning --bootstrap-server localhost:9092
```

## Connect to Cassandra
```bash
CASSANDRA_CONTAINER_ID=$(docker ps --filter "name=cassandra" --format "{{.ID}}")
docker exec -it "$CASSANDRA_CONTAINER_ID" cqlsh
CREATE KEYSPACE test_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE test_keyspace;
CREATE TABLE test_table (id UUID PRIMARY KEY, value TEXT);
```

## Run Flink job
```bash
mvn clean package
FLINK_CONTAINER_ID=$(docker ps --filter "name=flink-jobmanager" --format "{{.ID}}")
docker cp target/untitled-1.0-SNAPSHOT.jar "$FLINK_CONTAINER_ID":/opt/flink/flink-job.jar
docker exec -it "$FLINK_CONTAINER_ID" flink run /opt/flink/flink-job.jar
```

## Run Spark job

```bash
mvn clean package
SPARK_CONTAINER_ID=$(docker ps --filter "name=spark-master" --format "{{.ID}}")
docker cp target/spark-job.jar "$SPARK_CONTAINER_ID":/opt/spark/spark-job.jar
docker exec -it "$SPARK_CONTAINER_ID" spark-submit --class SparkKafkaToCassandra --master spark://spark-master:7077 /opt/spark/spark-job.jar
```
