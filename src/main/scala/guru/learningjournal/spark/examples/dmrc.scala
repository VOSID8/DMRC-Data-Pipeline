

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, from_json, to_timestamp}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.{Collections, Properties}
import java.util.regex.Pattern
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import scala.collection.JavaConversions._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._

import java.util
import java.util.Properties

object dmrc extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Stream Table Join Demo")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .config("spark.sql.shuffle.partitions", 2)
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", "9042")
      .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
      .config("spark.sql.catalog.lh", "com.datastax.spark.connector.datasource.CassandraCatalog")
      .getOrCreate()

    val loginSchema = StructType(List(
      StructField("charge", StringType),
      StructField("profile_id", StringType)
    ))

    val kafkaSourceDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "logins")
      .option("startingOffsets", "earliest")
      .load()

    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("group.id", "consumer-tutorial")
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    val kafkaConsumer = new KafkaConsumer[String, String](properties)
    kafkaConsumer.subscribe(util.Collections.singleton("dmrc"))

    while (true) {
      println("Reading kafka msgs")
      val results : ConsumerRecords[String, String] = kafkaConsumer.poll(2000)
      for (record <- results.iterator()) {
        println(s"Here's your $record")
      }
    val valueDF = kafkaSourceDF.select(from_json(col("value").cast("string"), loginSchema).alias("value"))
    
    val loginDF = valueDF.select("value.*")
    println(loginDF.toString)


    val userDF = spark.read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace","spark_db")
      .option("table","users")
      .load()
    
    val joinExpr = loginDF.col("profile_id") === userDF.col("profile_id")
    val joinedDF1 = loginDF.join(userDF, joinExpr, "inner").drop(userDF.col("profile_id"))
    val joinedDF = joinedDF1.withColumn("remaining_charge", col("remaining") - col("charge")).drop("remaining").withColumnRenamed("remaining_charge", "remaining")

    println(joinedDF.toString)
    val outputDF = joinedDF.select(col("profile_id"), col("age"),col("remaining"), col("user_name"))
    println("Checkpoint 1")
    println("outputDF: "+outputDF)
    
    // outputDF.write
    //   .format("org.apache.spark.sql.cassandra")
    //   .option("keyspace", "spark_db")
    //   .option("table", "users")
    //   .mode("append")
    //   .save();
    val outputQuery = outputDF.writeStream.format("org.apache.spark.sql.cassandra")
            .outputMode("append")
            .option("checkpointLocation", "chk-point-dir")
            .option("keyspace", "spark_db")
            .option("table", "users")
            .trigger(Trigger.ProcessingTime("1 minute"))
            .start()

    println("Checkpoint 2")

    logger.info("Waiting for Query")
    outputQuery.awaitTermination()

  }}




  // def writeToCassandra(): Unit = {
  //   val sparkMasterHost = "127.0.0.1"
  //   val cassandraHost = "127.0.0.1"
  //   val keyspace = "spark_db"
  //   val table = "users"

  //   // Tell Spark the address of one Cassandra node:
  //   val conf = new SparkConf(true).set("spark.cassandra.connection.host", cassandraHost)

  //   // Connect to the Spark cluster:
  //   val sc = new SparkContext("spark://" + sparkMasterHost + ":7077", "example", conf)

  //   // Read the table and print its contents:
  //   val rdd = sc.cassandraTable(keyspace, table)
  //   rdd.toArray().foreach(println)

  //   // Write two rows to the table:
  //   val col = sc.parallelize(Seq(("of", 1200), ("the", "863")))
  //   col.saveToCassandra(keyspace, table)

  //   sc.stop()
  // }
}