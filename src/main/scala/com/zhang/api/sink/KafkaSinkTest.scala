package com.zhang.api.sink

import java.util.Properties

import com.zhang.api.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}

/** *
 *
 * @author :  Brian
 *         Date:  2020/12/13
 *         Time: 11:00
 *         Description:
 **/
object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.server", "localhost:9092")
    properties.setProperty("group.id", "test-source")

    val stream01 = env.addSource(new FlinkKafkaConsumer010[String]("sensor", new SimpleStringSchema(), properties))

    val dataStream = stream01.map(data => {
      val attr = data.split(",")
      SensorReading(attr(0), attr(1).toLong, attr(2).toDouble).toString
    })

    dataStream.addSink(new FlinkKafkaProducer010[String]("localhost:9092", "sinkTest", new SimpleStringSchema()))

    env.execute()

  }

}
