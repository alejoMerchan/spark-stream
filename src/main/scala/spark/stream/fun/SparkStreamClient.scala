package spark.stream.fun

import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by ALEJANDRO on 15/11/2017.
  */
object SparkStreamClient extends App {


  val kafkaParams = Map(
    "zookeeper.connect" -> "localhost:2181",
    "group.id" -> "ulp-ch03-3.3",
    "zookeeper.connection.timeout.ms" -> "1000")


  val sparkConf = new SparkConf().setAppName("kafka-spark-test").setMaster("local[2]")
  val ssc = new StreamingContext(sparkConf, Seconds(2))

  val topics = "eventos-prueba"
  val topicMap = topics.split(",").map((_, 1)).toMap


  val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicMap,StorageLevel.MEMORY_ONLY_SER)

  stream.print()

  ssc.start()

  ssc.awaitTermination()


}
