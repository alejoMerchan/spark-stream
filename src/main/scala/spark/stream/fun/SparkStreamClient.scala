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


  val sparkConf = new SparkConf().setAppName("kafka-spark-test").setMaster("local[5]")
  val ssc = new StreamingContext(sparkConf, Seconds(1))

  val topics = "eventos-prueba"

  val stream = KafkaUtils.createStream(ssc, "localhost:2181" , "ulp-ch03-3.3", Map("eventos-prueba" -> 1))
  stream.cache()

  val filtroNegativos = stream.filter(x => x._2.split(":")(1).toInt < 0)

  filtroNegativos.foreachRDD {rdd =>
    if(rdd.isEmpty()) println("-----------------------rdd vacio")
    else rdd.foreach{
      x => procesoMensaje(x._2)
    }

  }

  ssc.start()

  ssc.awaitTermination()


  def procesoMensaje(mensaje:String): Unit ={
    println("procesando evento: " + mensaje)
  }


}
